#!/usr/bin/env node

import { createHash } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import {
  access,
  chmod,
  copyFile,
  cp,
  mkdir,
  open,
  readFile,
  readdir,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { basename, dirname, join, relative, resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const pinsRoot = join(repoRoot, "third_party", "codex-runtime");
const manifest = JSON.parse(
  await readFile(join(pinsRoot, "manifest.json"), "utf8"),
);
const target = process.argv[2];

if (!target || !manifest.codex.platforms[target]) {
  const supported = Object.keys(manifest.codex.platforms).join(", ");
  throw new Error(
    `usage: bundle-codex-runtime.mjs <target>; supported: ${supported}`,
  );
}

const platform = manifest.codex.platforms[target];
const cacheRoot = resolve(
  process.env.BUZZ_CODEX_RUNTIME_CACHE ??
    join(repoRoot, "target", "codex-runtime-cache"),
);
const outputRoot = join(
  repoRoot,
  "desktop",
  "src-tauri",
  "bundle-resources",
  "codex-cli",
);

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? repoRoot,
    encoding: "utf8",
    stdio: options.capture ? "pipe" : "inherit",
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    const detail = options.capture
      ? `\n${result.stdout ?? ""}${result.stderr ?? ""}`.trimEnd()
      : "";
    throw new Error(
      `${command} ${args.join(" ")} failed (${result.status})${detail}`,
    );
  }
  return options.capture ? (result.stdout ?? "").trim() : "";
}

async function digest(path, algorithm, encoding) {
  const hash = createHash(algorithm);
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest(encoding);
}

async function sha256(path) {
  return digest(path, "sha256", "hex");
}

async function verifySha256(path, expected, label) {
  const actual = await sha256(path);
  if (actual !== expected) {
    throw new Error(
      `${label} SHA-256 mismatch: expected ${expected}, got ${actual}`,
    );
  }
}

async function download(url, destination) {
  try {
    await access(destination);
    return;
  } catch {
    // Download below.
  }
  await mkdir(dirname(destination), { recursive: true });
  const partial = `${destination}.part`;
  await rm(partial, { force: true });
  const response = await fetch(url, { redirect: "follow" });
  if (!response.ok || !response.body) {
    throw new Error(`download failed (${response.status}) for ${url}`);
  }
  await pipeline(Readable.fromWeb(response.body), createWriteStream(partial));
  await rename(partial, destination);
}

function hostRustTarget() {
  return {
    "darwin-arm64": "aarch64-apple-darwin",
    "darwin-x64": "x86_64-apple-darwin",
    "linux-arm64": "aarch64-unknown-linux-gnu",
    "linux-x64": "x86_64-unknown-linux-gnu",
    "win32-arm64": "aarch64-pc-windows-msvc",
    "win32-x64": "x86_64-pc-windows-msvc",
  }[`${process.platform}-${process.arch}`];
}

async function readAt(path, length, position) {
  const handle = await open(path, "r");
  try {
    const buffer = Buffer.alloc(length);
    const { bytesRead } = await handle.read(buffer, 0, length, position);
    if (bytesRead !== length)
      throw new Error(`short binary header (${bytesRead}/${length})`);
    return buffer;
  } finally {
    await handle.close();
  }
}

async function assertBinaryArchitecture(path, rustTarget) {
  try {
    const header = await readAt(path, 64, 0);
    if (rustTarget.includes("windows")) {
      if (header.toString("ascii", 0, 2) !== "MZ")
        throw new Error("missing MZ header");
      const pe = await readAt(path, 6, header.readUInt32LE(0x3c));
      if (pe.toString("binary", 0, 4) !== "PE\0\0")
        throw new Error("missing PE signature");
      const expected = rustTarget.startsWith("x86_64") ? 0x8664 : 0xaa64;
      const machine = pe.readUInt16LE(4);
      if (machine !== expected)
        throw new Error(`PE machine 0x${machine.toString(16)}`);
    } else if (rustTarget.includes("apple")) {
      const magic = header.readUInt32LE(0);
      if (magic !== 0xfeedfacf)
        throw new Error(`Mach-O magic 0x${magic.toString(16)}`);
      const expected = rustTarget.startsWith("x86_64")
        ? 0x01000007
        : 0x0100000c;
      const cpu = header.readUInt32LE(4);
      if (cpu !== expected) throw new Error(`Mach-O cpu 0x${cpu.toString(16)}`);
    } else {
      if (header.toString("hex", 0, 4) !== "7f454c46")
        throw new Error("missing ELF header");
      const expected = rustTarget.startsWith("x86_64") ? 0x3e : 0xb7;
      const machine = header.readUInt16LE(18);
      if (machine !== expected)
        throw new Error(`ELF machine 0x${machine.toString(16)}`);
    }
  } catch (error) {
    throw new Error(`${path} does not match ${rustTarget}: ${error.message}`);
  }
}

async function stageCodexRuntime() {
  const packageName = `codex-${manifest.codex.version}-${platform.packageSuffix}.tgz`;
  const packageUrl = `https://registry.npmjs.org/@openai/codex/-/${packageName}`;
  const archive = join(cacheRoot, packageName);
  await download(packageUrl, archive);
  const actualIntegrity = `sha512-${await digest(archive, "sha512", "base64")}`;
  if (actualIntegrity !== platform.integrity) {
    throw new Error(
      `Codex package integrity mismatch: expected ${platform.integrity}`,
    );
  }

  const extractRoot = join(
    cacheRoot,
    `codex-${manifest.codex.version}-${platform.packageSuffix}`,
  );
  await rm(extractRoot, { recursive: true, force: true });
  await mkdir(extractRoot, { recursive: true });
  run("tar", ["-xzf", archive, "-C", extractRoot]);

  const vendorRoot = join(
    extractRoot,
    "package",
    "vendor",
    platform.vendorTriple,
  );
  const runtimeOutput = join(outputRoot, "runtime");
  await cp(vendorRoot, runtimeOutput, { recursive: true, force: true });

  const extension = target.includes("windows") ? ".exe" : "";
  const required = [
    `bin/codex${extension}`,
    `bin/codex-code-mode-host${extension}`,
    `codex-path/rg${extension}`,
    "codex-package.json",
  ];
  if (target.includes("windows")) {
    required.push(
      "codex-resources/codex-command-runner.exe",
      "codex-resources/codex-windows-sandbox-setup.exe",
    );
  } else if (target.includes("apple")) {
    required.push("codex-resources/zsh/bin/zsh");
  }
  for (const relativePath of required) {
    const path = join(runtimeOutput, relativePath);
    const info = await stat(path);
    if (!info.isFile())
      throw new Error(`missing Codex runtime payload: ${relativePath}`);
    if (relativePath !== "codex-package.json") {
      if (!target.includes("windows")) await chmod(path, 0o755);
      await assertBinaryArchitecture(path, target);
    }
  }

  const codexPath = join(runtimeOutput, `bin/codex${extension}`);
  if (hostRustTarget() === target) {
    const version = run(codexPath, ["--version"], { capture: true });
    if (version !== `codex-cli ${manifest.codex.version}`) {
      throw new Error(`unexpected Codex version output: ${version}`);
    }
  }
  return { codexPath, packageUrl, actualIntegrity, required };
}

async function listFiles(root) {
  const files = [];
  async function visit(directory) {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isSymbolicLink())
        throw new Error(`symlink not allowed in Codex payload: ${path}`);
      if (entry.isDirectory()) await visit(path);
      else if (entry.isFile()) files.push(path);
    }
  }
  await visit(root);
  return files.sort();
}

async function main() {
  await mkdir(outputRoot, { recursive: true });
  for (const entry of await readdir(outputRoot)) {
    if (entry !== ".keep")
      await rm(join(outputRoot, entry), { recursive: true, force: true });
  }

  const licenseSource = join(pinsRoot, manifest.codex.license);
  await verifySha256(
    licenseSource,
    manifest.codex.licenseSha256,
    "OpenAI Codex license",
  );
  const runtime = await stageCodexRuntime();
  await mkdir(join(outputRoot, "licenses"), { recursive: true });
  await copyFile(
    licenseSource,
    join(outputRoot, "licenses", "openai-codex-LICENSE"),
  );

  const payloads = [];
  for (const path of await listFiles(outputRoot)) {
    if (basename(path) === ".keep") continue;
    payloads.push({
      path: relative(outputRoot, path).replaceAll("\\", "/"),
      bytes: (await stat(path)).size,
      sha256: await sha256(path),
    });
  }
  const provenance = {
    schemaVersion: 1,
    target,
    codexPath: relative(outputRoot, runtime.codexPath).replaceAll("\\", "/"),
    codex: {
      version: manifest.codex.version,
      upstreamCommit: manifest.codex.upstreamCommit,
      packageUrl: runtime.packageUrl,
      integrity: runtime.actualIntegrity,
      requiredPayloads: runtime.required,
      licenseSha256: manifest.codex.licenseSha256,
    },
    payloads,
  };
  await writeFile(
    join(outputRoot, "PROVENANCE.json"),
    `${JSON.stringify(provenance, null, 2)}\n`,
  );

  const checksums = [];
  for (const path of await listFiles(outputRoot)) {
    if ([".keep", "SHA256SUMS.txt"].includes(basename(path))) continue;
    checksums.push(
      `${await sha256(path)}  ${relative(outputRoot, path).replaceAll("\\", "/")}`,
    );
  }
  await writeFile(
    join(outputRoot, "SHA256SUMS.txt"),
    `${checksums.join("\n")}\n`,
  );
  console.log(`Bundled verified Codex CLI for ${target} -> ${outputRoot}`);
}

await main();
