// Modified by Intent: CLI and login use the same device runtime as app-server.
import type {ChildProcess, SpawnOptions} from "node:child_process";
import {spawn} from "node:child_process";
import {resolveHostCodex} from "./HostCodex";

export function runCodexCli(codexPath: string | undefined, args: Array<string>): Promise<number> {
    const child = spawnCodexCli(codexPath, args);

    return new Promise((resolve, reject) => {
        child.on("error", reject);
        child.on("exit", (code, signal) => {
            if (signal) {
                process.kill(process.pid, signal);
                return;
            }
            resolve(code ?? 1);
        });
    });
}

function spawnCodexCli(codexPath: string | undefined, args: Array<string>): ChildProcess {
    const options: SpawnOptions = {
        env: process.env,
        stdio: "inherit",
    };

    const host = resolveHostCodex(process.env, codexPath);
    return spawn(host, args, {...options, shell: process.platform === "win32"});
}
