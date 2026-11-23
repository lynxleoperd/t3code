// @effect-diagnostics nodeBuiltinImport:off - The CLI bridge uses a loopback socket and verifies its peer through macOS lsof.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeUtil from "node:util";
import xterm from "@xterm/headless";
import {
  AntigravitySettings,
  CloudSpeechError,
  resolveProviderInstanceEnabled,
  type ProviderInstanceId,
  type ServerSettingsError,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { PtyAdapter, type PtyProcess } from "@t3tools/shared/PtyAdapter";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import { ServerConfig } from "../config.ts";
import { resolveAntigravityInstanceDirectories } from "../provider/antigravityAuthSupport.ts";
import { AntigravityInstallation } from "../provider/AntigravityInstallation.ts";
import { ServerSettingsService } from "../serverSettings.ts";

const isCloudSpeechError = Schema.is(CloudSpeechError);
const decodeAntigravitySettings = Schema.decodeUnknownOption(AntigravitySettings);
const exec = NodeUtil.promisify(NodeChildProcess.execFile);
const BYTES_PER_SECOND = 16_000 * 2;
const FRAME_MS = 20;
const FRAME_BYTES = (BYTES_PER_SECOND * FRAME_MS) / 1_000;
const TERMINAL_COLS = 120;
const TERMINAL_ROWS = 32;
const DICTATION_KEY = "\x1b[15~";
const EDITOR_KEY = "\x07";
const accountUnavailable = () =>
  new CloudSpeechError({ provider: "antigravity", reason: "account-unavailable" });
const unavailable = () => new CloudSpeechError({ provider: "antigravity", reason: "unavailable" });

export function supportsAntigravitySpeech(platform: string, architecture: string) {
  return platform === "darwin" && architecture === "arm64";
}

function missingFile(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "ENOENT";
}

function shellQuote(argument: string): string {
  return `'${argument.replaceAll("'", "'\\''")}'`;
}

async function prepareSession(root: string, credentialPath: string, baseEnv: NodeJS.ProcessEnv) {
  const geminiHome = NodePath.join(root, "gemini");
  const cliState = NodePath.join(geminiHome, "antigravity-cli");
  const workspace = NodePath.join(root, "workspace");
  const runtimeTemp = NodePath.join(root, "tmp");
  await Promise.all([
    NodeFSP.mkdir(cliState, { recursive: true, mode: 0o700 }),
    NodeFSP.mkdir(workspace),
    NodeFSP.mkdir(runtimeTemp),
  ]);
  // Antigravity handles its own OAuth. Its temporary session can refresh independently of T3's live profile.
  const credential = NodePath.join(cliState, "antigravity-oauth-token");
  await NodeFSP.copyFile(credentialPath, credential, NodeFS.constants.COPYFILE_EXCL);
  await NodeFSP.chmod(credential, 0o600);

  const draft = NodePath.join(root, "draft.txt");
  const pending = NodePath.join(root, "draft.pending");
  const editor = NodePath.join(root, "export-draft");
  await Promise.all([
    NodeFSP.writeFile(
      editor,
      [
        "#!/bin/sh",
        "set -eu",
        "for document; do :; done",
        `/bin/cp "$document" ${shellQuote(pending)}`,
        `/bin/mv ${shellQuote(pending)} ${shellQuote(draft)}`,
        "",
      ].join("\n"),
      { mode: 0o700 },
    ),
    NodeFSP.writeFile(
      NodePath.join(cliState, "settings.json"),
      JSON.stringify({ auth: { type: "oauth-personal" }, editor }),
    ),
  ]);

  const env: Record<string, string> = {};
  for (const key of ["HOME", "LANG", "LOGNAME", "PATH", "USER"]) {
    const value = baseEnv[key];
    if (value !== undefined) env[key] = value;
  }
  return {
    workspace,
    draft,
    env: {
      ...env,
      AGY_ACP_FORCE_FILE_STORAGE: "1",
      AGY_CLI_DISABLE_AUTO_UPDATE: "1",
      ANTIGRAVITY_APP_DATA_DIR: cliState,
      ANTIGRAVITY_GEMINI_DIR: geminiHome,
      BROWSER: "/usr/bin/false",
      EDITOR: editor,
      GEMINI_HOME: geminiHome,
      // Use the CLI's remote microphone path for the recording over loopback.
      SSH_CONNECTION: "127.0.0.1 0 127.0.0.1 0",
      TERM: "xterm-256color",
      TMPDIR: runtimeTemp,
      VISUAL: editor,
    },
  };
}

async function openRelay(recording: Buffer) {
  const controller = new AbortController();
  const candidates = new Set<NodeNet.Socket>();
  let peer: number | undefined;
  let connection: NodeNet.Socket | undefined;
  let deliveredAt: number | undefined;
  let failure: Error | undefined;

  const server = NodeNet.createServer((socket) => {
    if (connection || peer === undefined || candidates.size >= 4) {
      socket.destroy();
      return;
    }
    candidates.add(socket);
    socket.on("error", (error) => {
      if (socket === connection && deliveredAt === undefined && !controller.signal.aborted) {
        failure = error;
      }
    });
    socket.on("close", () => {
      candidates.delete(socket);
      if (socket === connection && deliveredAt === undefined && !controller.signal.aborted) {
        failure ??= new Error(
          "Antigravity closed the microphone relay before receiving the recording.",
        );
      }
    });
    const pid = peer;
    void (async () => {
      // ANTIGRAVITY_MIC is a plain TCP endpoint. Check the other end before sending
      // microphone audio so an unrelated local process cannot receive the recording.
      const endpoint = `n127.0.0.1:${socket.remotePort}->127.0.0.1:${socket.localPort}`;
      const result = await exec(
        "/usr/sbin/lsof",
        ["-nP", "-a", "-p", String(pid), "-iTCP", "-sTCP:ESTABLISHED", "-Fn"],
        { signal: controller.signal, timeout: 2_000 },
      ).catch(() => undefined);
      if (connection || socket.destroyed || !result?.stdout.split("\n").includes(endpoint)) {
        socket.destroy();
        return;
      }
      connection = socket;
      for (let offset = 0; offset < recording.length; offset += FRAME_BYTES) {
        controller.signal.throwIfAborted();
        if (socket.destroyed) throw new Error("The microphone relay was closed.");
        await new Promise<void>((resolve, reject) => {
          socket.write(recording.subarray(offset, offset + FRAME_BYTES), (error) => {
            if (error) reject(error);
            else resolve();
          });
        });
        await NodeTimersPromises.setTimeout(FRAME_MS, undefined, { signal: controller.signal });
      }
      deliveredAt = performance.now();
    })().catch((error: unknown) => {
      if (!controller.signal.aborted) {
        failure = error instanceof Error ? error : new Error("The microphone relay failed.");
      }
    });
  });
  server.on("error", (error) => {
    failure = error;
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("Could not open the local microphone relay.");
  }
  return {
    address: `127.0.0.1:${address.port}`,
    expectPeer(pid: number) {
      peer = pid;
    },
    get deliveredAt() {
      return deliveredAt;
    },
    get failure() {
      return failure;
    },
    async close() {
      controller.abort();
      for (const socket of candidates) socket.destroy();
      await new Promise<void>((done) => server.close(() => done()));
    },
  };
}

type Relay = Awaited<ReturnType<typeof openRelay>>;

async function collectDraft(
  terminal: xterm.Terminal,
  child: PtyProcess,
  draft: string,
  relay: Relay,
  durationMs: number,
  isExited: () => boolean,
  signal?: AbortSignal,
): Promise<string> {
  const deadline = performance.now() + durationMs + 38_000;
  let state = "starting Antigravity";
  let microphoneRequested = false;
  let stopped = false;
  let settledAt: number | undefined;
  let editorRequested = false;

  while (performance.now() < deadline && !isExited()) {
    signal?.throwIfAborted();
    if (relay.failure) throw relay.failure;
    const buffer = terminal.buffer.active;
    const lines = Array.from({ length: TERMINAL_ROWS }, (_, row) =>
      (buffer.getLine(buffer.viewportY + row)?.translateToString(true) ?? "").trim().toLowerCase(),
    );
    const composerReady = lines.some((line) => line.startsWith("? for shortcuts"));
    const recordingActive = lines.some((line) => /^[●○]\s+recording\s+\d{2}:\d{2}\b/u.test(line));
    const now = performance.now();
    // After recording starts the terminal also shows the user's words. Treat only CLI notices as errors.
    if (
      !microphoneRequested &&
      lines.some((line) => line.includes("sign in with google") || line.includes("login required"))
    ) {
      throw accountUnavailable();
    }
    if (lines.some((line) => /^⎿\s+transcription failed:/u.test(line))) throw unavailable();
    if (lines.some((line) => /^⎿\s+no speech detected\b/u.test(line))) return "";
    if (!microphoneRequested && composerReady) {
      child.write(DICTATION_KEY);
      microphoneRequested = true;
      state = "connecting the microphone relay";
    }
    if (!stopped && relay.deliveredAt !== undefined && now - relay.deliveredAt >= 700) {
      child.write(DICTATION_KEY);
      stopped = true;
      state = "waiting for the final draft";
    }
    if (stopped && !recordingActive && composerReady) {
      settledAt ??= now;
      if (!editorRequested && now - settledAt >= 700) {
        // The editor shortcut gives us the exact draft without scraping text or submitting a turn.
        child.write(EDITOR_KEY);
        editorRequested = true;
        state = "exporting the final draft";
      }
    } else {
      settledAt = undefined;
    }
    if (editorRequested) {
      try {
        const text = (await NodeFSP.readFile(draft, "utf8")).trim();
        return text;
      } catch (error) {
        if (!missingFile(error)) throw error;
      }
    }
    await NodeTimersPromises.setTimeout(75, undefined, { signal });
  }
  throw new Error(`Antigravity dictation did not complete while ${state}.`);
}

async function useAntigravity(
  recording: Buffer,
  executable: string,
  credentialPath: string,
  baseEnv: NodeJS.ProcessEnv,
  pty: PtyAdapter["Service"],
  signal: AbortSignal,
): Promise<string> {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-antigravity-dictation-"));
  try {
    const { workspace, draft, env } = await prepareSession(root, credentialPath, baseEnv);
    signal?.throwIfAborted();
    const relay = await openRelay(recording);
    try {
      const child = await Effect.runPromise(
        pty.spawn({
          shell: executable,
          args: ["--new-project", "--sandbox"],
          cwd: workspace,
          env: { ...env, ANTIGRAVITY_MIC: relay.address },
          cols: TERMINAL_COLS,
          rows: TERMINAL_ROWS,
        }),
      );
      relay.expectPeer(child.pid);
      const terminal = new xterm.Terminal({
        cols: TERMINAL_COLS,
        rows: TERMINAL_ROWS,
        allowProposedApi: true,
      });
      let exited = false;
      const exit = new Promise<void>((done) => {
        child.onExit(() => {
          exited = true;
          done();
        });
      });
      terminal.onData((data) => {
        if (!exited) child.write(data);
      });
      child.onData((data) => terminal.write(data));
      // Antigravity's startup probe waits for this xterm capability response.
      terminal.parser.registerCsiHandler({ prefix: ">", final: "c" }, () => {
        if (!exited) child.write("\x1b[>0;390;0c");
        return true;
      });
      try {
        return await collectDraft(
          terminal,
          child,
          draft,
          relay,
          (recording.length / BYTES_PER_SECOND) * 1_000,
          () => exited,
          signal,
        );
      } finally {
        if (!exited) {
          if (signal?.aborted) child.write("\x1b");
          child.kill("SIGHUP");
          await Promise.race([
            exit,
            NodeTimersPromises.setTimeout(3_000, undefined, { ref: false }),
          ]);
          if (!exited) {
            child.kill("SIGKILL");
            await Promise.race([
              exit,
              NodeTimersPromises.setTimeout(1_000, undefined, { ref: false }),
            ]);
          }
        }
        terminal.dispose();
      }
    } finally {
      await relay.close();
    }
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true, maxRetries: 3 });
  }
}

export class AntigravitySpeech extends Context.Service<
  AntigravitySpeech,
  {
    readonly account: Effect.Effect<ProviderInstanceId | undefined, ServerSettingsError>;
    readonly transcribe: (
      instanceId: ProviderInstanceId | undefined,
      recording: Buffer,
    ) => Effect.Effect<string, CloudSpeechError | ServerSettingsError>;
  }
>()("t3/speech/AntigravitySpeech") {}

export const layer = Layer.effect(
  AntigravitySpeech,
  Effect.gen(function* () {
    const platform = yield* HostProcess.Platform;
    const architecture = yield* HostProcess.Architecture;
    const baseEnv = yield* HostProcess.Environment;
    const { stateDir } = yield* ServerConfig;
    const settings = yield* ServerSettingsService;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const digest = yield* Digest.Digest;
    const installation = yield* AntigravityInstallation;
    const pty = yield* PtyAdapter;
    const gate = yield* Semaphore.make(1);

    const configuredAccount = Effect.gen(function* () {
      if (!supportsAntigravitySpeech(platform, architecture)) return undefined;
      const { providerInstances } = yield* settings.getSettings;
      const matching = [];
      for (const [instanceId, instance] of Object.entries(providerInstances)) {
        if (instance.driver !== "antigravity" || !resolveProviderInstanceEnabled(instance))
          continue;
        const config = decodeAntigravitySettings(instance.config ?? {});
        if (Option.isNone(config) || config.value.authMethod !== "oauth-personal") continue;
        const directories = yield* resolveAntigravityInstanceDirectories(
          stateDir,
          instanceId as ProviderInstanceId,
        ).pipe(
          Effect.provideService(Path.Path, path),
          Effect.provideService(Digest.Digest, digest),
          Effect.option,
        );
        if (Option.isNone(directories)) continue;
        const credential = path.join(
          directories.value.profile,
          "antigravity-acp",
          "antigravity-oauth-token",
        );
        if (!(yield* fs.exists(credential).pipe(Effect.orElseSucceed(() => false)))) continue;
        matching.push({
          instanceId: instanceId as ProviderInstanceId,
          credential,
          binaryPath: config.value.binaryPath,
        });
      }
      // The menu represents one Google account. Do not guess when several are connected.
      return matching.length === 1 ? matching[0] : undefined;
    });
    const transcribe = Effect.fn("AntigravitySpeech.transcribe")(
      function* (instanceId: ProviderInstanceId | undefined, recording: Buffer) {
        const account = yield* configuredAccount;
        if (!account || account.instanceId !== instanceId) return yield* accountUnavailable();
        const executable = yield* installation
          .acquire(account.binaryPath, baseEnv)
          .pipe(Effect.mapError(unavailable));
        return yield* Effect.callback<string, CloudSpeechError>((resume, signal) => {
          const pending = useAntigravity(
            recording,
            executable.executablePath,
            account.credential,
            baseEnv,
            pty,
            signal,
          ).then(
            (text) => resume(Effect.succeed(text)),
            (cause: unknown) =>
              resume(Effect.fail(isCloudSpeechError(cause) ? cause : unavailable())),
          );
          // Wait for PTY and relay cleanup before releasing the executable lease on interruption.
          return Effect.promise(() => pending);
        });
      },
      (effect) => gate.withPermit(Effect.scoped(effect)),
    );
    return AntigravitySpeech.of({
      account: configuredAccount.pipe(Effect.map((account) => account?.instanceId)),
      transcribe,
    });
  }),
);
