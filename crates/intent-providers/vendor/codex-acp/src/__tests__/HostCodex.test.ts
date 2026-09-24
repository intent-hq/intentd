import {afterEach, describe, expect, it} from "vitest";
import {mkdtempSync, mkdirSync, rmSync, writeFileSync} from "node:fs";
import {tmpdir} from "node:os";
import {delimiter, join} from "node:path";
import {resolveHostCodex} from "../HostCodex";

const dirs: string[] = [];
afterEach(() => dirs.splice(0).forEach(dir => rmSync(dir, {recursive: true, force: true})));
describe("device Codex resolution", () => {
    it("follows the selected device installation, including paths with spaces", () => {
        const root = mkdtempSync(join(tmpdir(), "host codex "));
        dirs.push(root);
        const filename = process.platform === "win32" ? "codex.exe" : "codex";
        const first = join(root, "first");
        const second = join(root, "second");
        mkdirSync(first);
        mkdirSync(second);
        writeFileSync(join(second, filename), "runtime", {mode: 0o755});
        const env = {PATH: [first, second].join(delimiter)};
        expect(resolveHostCodex(env)).toBe(join(second, filename));
        writeFileSync(join(first, filename), "new runtime", {mode: 0o755});
        expect(resolveHostCodex(env)).toBe(join(first, filename));
    });
    it("fails clearly without falling back to a packaged Codex", () => {
        expect(() => resolveHostCodex({PATH: ""})).toThrow("Codex CLI not found");
    });
    it("does not search the workspace through empty or relative PATH entries", () => {
        expect(() => resolveHostCodex({PATH: ["", ".", "node_modules/.bin"].join(delimiter)})).toThrow("Codex CLI not found");
    });
});
