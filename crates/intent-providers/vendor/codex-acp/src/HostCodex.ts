import {accessSync, constants, statSync} from "node:fs";
import {delimiter, isAbsolute, join} from "node:path";

export function resolveHostCodex(env: NodeJS.ProcessEnv = process.env, explicit?: string): string {
    const names = process.platform === "win32" ? ["codex.exe", "codex.cmd", "codex.bat"] : ["codex"];
    const candidates = explicit ? [explicit] : (env["PATH"] ?? "").split(delimiter)
        .filter(isAbsolute)
        .flatMap(dir => names.map(name => join(dir, name)));
    for (const candidate of candidates) {
        try {
            accessSync(candidate, constants.X_OK);
            if (statSync(candidate).isFile()) return candidate;
        } catch {
            // Continue past stale PATH entries.
        }
    }
    throw new Error("Codex CLI not found on this device. Install Codex and ensure codex is on PATH, then start a new session.");
}
