import {
  CloudSpeechError,
  CodexSettings,
  EnvironmentSpeechTranscriptionResult,
  isUnconfiguredDefaultInstanceEnabled,
  ProviderDriverKind,
  ProviderInstanceId,
  resolveProviderInstanceEnabled,
  type ServerSettingsError,
} from "@t3tools/contracts";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import { ServerConfig } from "../config.ts";
import { resolveManagedCodexHomeLayout } from "../provider/CodexManagedHome.ts";
import { resolveCodexHomeLayout } from "../provider/Drivers/CodexHomeLayout.ts";
import { ServerSettingsService } from "../serverSettings.ts";

// This ChatGPT service has no public API contract. Keep the credential bound to this origin.
const TRANSCRIBE_URL = "https://chatgpt.com/backend-api/transcribe";
const defaultInstanceId = ProviderInstanceId.make("codex");
const HeaderValue = Schema.String.check(Schema.isPattern(/^[!-~]+$/));
const decodeAuth = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      auth_mode: Schema.Literal("chatgpt"),
      OPENAI_API_KEY: Schema.optionalKey(Schema.Null),
      tokens: Schema.Struct({ access_token: HeaderValue, account_id: HeaderValue }),
    }),
  ),
);
const decodeSettings = Schema.decodeUnknownOption(CodexSettings);
const accountUnavailable = () =>
  new CloudSpeechError({ provider: "codex", reason: "account-unavailable" });
const unavailable = () => new CloudSpeechError({ provider: "codex", reason: "unavailable" });

export class CodexSpeech extends Context.Service<
  CodexSpeech,
  {
    readonly account: Effect.Effect<ProviderInstanceId | undefined, ServerSettingsError>;
    readonly transcribe: (
      instanceId: ProviderInstanceId,
      audio: ArrayBuffer,
    ) => Effect.Effect<string, CloudSpeechError | ServerSettingsError>;
  }
>()("t3/speech/CodexSpeech") {}

export const layer = Layer.effect(
  CodexSpeech,
  Effect.gen(function* () {
    const settings = yield* ServerSettingsService;
    const { stateDir } = yield* ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const platform = yield* HostProcess.Platform;
    const baseEnv = yield* HostProcess.Environment;
    const http = yield* HttpClient.HttpClient;

    const configuredAccount = Effect.gen(function* () {
      const { providerInstances } = yield* settings.getSettings;
      const entries = Object.entries(providerInstances).filter(
        ([, item]) => item.driver === "codex" && resolveProviderInstanceEnabled(item),
      );
      if (
        !Object.hasOwn(providerInstances, defaultInstanceId) &&
        isUnconfiguredDefaultInstanceEnabled(defaultInstanceId)
      ) {
        entries.push([defaultInstanceId, { driver: ProviderDriverKind.make("codex") }]);
      }
      const accounts = [];
      for (const [rawId, instance] of entries) {
        const instanceId = ProviderInstanceId.make(rawId);
        const config = decodeSettings(instance.config ?? {});
        if (Option.isNone(config)) continue;
        // A secret override that could change the home must not fall back to another account.
        if (
          instance.environment?.some(
            (variable) =>
              variable.name.toUpperCase() === "CODEX_HOME" && typeof variable.value !== "string",
          )
        )
          continue;
        const env = yield* mergeProviderInstanceEnvironment(instance.environment, baseEnv);
        const environmentHome = env.CODEX_HOME?.trim();
        const layout =
          config.value.setupMode === "managed"
            ? yield* resolveManagedCodexHomeLayout(stateDir, instanceId, config.value)
            : yield* resolveCodexHomeLayout(
                config.value.homePath || !environmentHome
                  ? config.value
                  : { ...config.value, homePath: environmentHome },
              );
        const authPath = path.join(layout.effectiveHomePath ?? layout.sharedHomePath, "auth.json");
        const info = yield* fs.stat(authPath).pipe(Effect.option);
        if (
          Option.isNone(info) ||
          info.value.type !== "File" ||
          info.value.size > 262_144n ||
          (platform !== "win32" && (info.value.mode & 0o077) !== 0)
        )
          continue;
        const auth = yield* fs.readFileString(authPath).pipe(Effect.map(decodeAuth), Effect.option);
        if (Option.isNone(auth) || Option.isNone(auth.value)) continue;
        accounts.push({
          instanceId,
          token: Redacted.make(auth.value.value.tokens.access_token),
          accountId: auth.value.value.tokens.account_id,
        });
      }
      return accounts.length === 1 ? accounts[0] : undefined;
    }).pipe(Effect.provideService(Path.Path, path));

    const transcribe = Effect.fn("CodexSpeech.transcribe")(function* (
      instanceId: ProviderInstanceId,
      audio: ArrayBuffer,
    ) {
      const account = yield* configuredAccount;
      if (!account || account.instanceId !== instanceId) return yield* accountUnavailable();
      const form = new FormData();
      form.set("file", new Blob([audio], { type: "audio/wav" }), "dictation.wav");
      const request = HttpClientRequest.post(TRANSCRIBE_URL).pipe(
        HttpClientRequest.setHeader("originator", "t3code"),
        HttpClientRequest.setHeader("chatgpt-account-id", account.accountId),
        HttpClientRequest.bearerToken(account.token),
        HttpClientRequest.bodyFormData(form),
      );
      const response = yield* http.execute(request).pipe(
        Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
        Effect.provideService(HttpClient.TracerPropagationEnabled, false),
        Effect.withTracerEnabled(false),
        Effect.tapError((error) =>
          Effect.logDebug("Codex dictation HTTP transport failed", { reason: error.reason._tag }),
        ),
        Effect.mapError(unavailable),
      );
      yield* Effect.logDebug("Codex dictation upstream status", { status: response.status });
      if (response.status === 401 || response.status === 403) return yield* accountUnavailable();
      if (response.status === 429)
        return yield* new CloudSpeechError({ provider: "codex", reason: "rate-limit" });
      if (response.status < 200 || response.status >= 300) return yield* unavailable();
      const { text } = yield* HttpClientResponse.schemaBodyJson(
        EnvironmentSpeechTranscriptionResult,
      )(response).pipe(Effect.withTracerEnabled(false), Effect.mapError(unavailable));
      return text;
    });
    return CodexSpeech.of({
      account: configuredAccount.pipe(Effect.map((account) => account?.instanceId)),
      transcribe,
    });
  }),
);
