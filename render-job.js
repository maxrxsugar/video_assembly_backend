const fs = require("fs");
const os = require("os");
const path = require("path");
const { execSync } = require("child_process");

async function main() {
  const productName = process.env.PRODUCT_NAME || "product";
  const hookClipUrl = process.env.HOOK_CLIP_URL || "";
  const scienceClipUrl = process.env.SCIENCE_CLIP_URL || "";
  const productClipUrl = process.env.PRODUCT_CLIP_URL || "";
  const narrationAudioUrl = process.env.NARRATION_AUDIO_URL || "";

  const introBumperLink = process.env.INTRO_BUMPER || "";
  const outroBumperLink = process.env.OUTRO_BUMPER || "";
  const logoOverlayLink = process.env.LOGO_OVERLAY || "";

  const outputGcsUri = process.env.OUTPUT_GCS_URI || "";

  if (!hookClipUrl || !scienceClipUrl || !productClipUrl || !narrationAudioUrl) {
    throw new Error("Missing one or more clip/audio URLs");
  }

  if (!introBumperLink || !outroBumperLink || !logoOverlayLink) {
    throw new Error("Missing CONFIG asset links");
  }

  if (!outputGcsUri) {
    throw new Error("Missing OUTPUT_GCS_URI");
  }

  const accessToken = await getAccessToken();
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "assemble-"));

  const introPath = path.join(workDir, "intro.mp4");
  const hookPath = path.join(workDir, "hook.mp4");
  const sciencePath = path.join(workDir, "science.mp4");
  const productPath = path.join(workDir, "product.mp4");
  const outroPath = path.join(workDir, "outro.mp4");

  const narrationPath = path.join(workDir, "narration.mp3");
  const logoPath = path.join(workDir, "logo.png");

  const hookNorm = path.join(workDir, "hook_norm.mp4");
  const scienceNorm = path.join(workDir, "science_norm.mp4");
  const productNorm = path.join(workDir, "product_norm.mp4");

  const introNorm = path.join(workDir, "intro_norm.mp4");
  const outroNorm = path.join(workDir, "outro_norm.mp4");

  const middleConcat = path.join(workDir, "middle.mp4");
  const middleNarr = path.join(workDir, "middle_narr.mp4");

  const finalConcat = path.join(workDir, "final_no_logo.mp4");
  const finalOutput = path.join(workDir, "final_output.mp4");

  const concatFile = path.join(workDir, "concat.txt");
  const finalConcatFile = path.join(workDir, "final_concat.txt");

  console.log("Downloading static assets...");
  await downloadDriveFile(introBumperLink, introPath);
  await downloadDriveFile(outroBumperLink, outroPath);
  await downloadDriveFile(logoOverlayLink, logoPath);

  console.log("Downloading generated assets...");
  await downloadGcsFile(hookClipUrl, hookPath, accessToken);
  await downloadGcsFile(scienceClipUrl, sciencePath, accessToken);
  await downloadGcsFile(productClipUrl, productPath, accessToken);
  await downloadGcsFile(narrationAudioUrl, narrationPath, accessToken);

  console.log("Normalizing videos...");
  normalizeVideoVideoOnly(hookPath, hookNorm);
  normalizeVideoVideoOnly(sciencePath, scienceNorm);
  normalizeVideoVideoOnly(productPath, productNorm);
  normalizeVideoVideoOnly(introPath, introNorm);
  normalizeVideoVideoOnly(outroPath, outroNorm);

  console.log("Concatenating middle clips...");
  fs.writeFileSync(
    concatFile,
    [
      `file '${hookNorm}'`,
      `file '${scienceNorm}'`,
      `file '${productNorm}'`
    ].join("\n")
  );

  runCommand(
    `ffmpeg -y -f concat -safe 0 -i "${concatFile}" -c:v libx264 -pix_fmt yuv420p -an "${middleConcat}"`,
    "Concatenating middle clips failed"
  );

  console.log("Adding narration...");
  runCommand(
    `ffmpeg -y -i "${middleConcat}" -i "${narrationPath}" -map 0:v:0 -map 1:a:0 -c:v copy -c:a aac -shortest "${middleNarr}"`,
    "Muxing narration onto middle section failed"
  );

  console.log("Concatenating intro + middle + outro...");
  fs.writeFileSync(
    finalConcatFile,
    [
      `file '${introNorm}'`,
      `file '${middleNarr}'`,
      `file '${outroNorm}'`
    ].join("\n")
  );

  runCommand(
    `ffmpeg -y -f concat -safe 0 -i "${finalConcatFile}" -c:v libx264 -pix_fmt yuv420p -c:a aac "${finalConcat}"`,
    "Final concatenation failed"
  );

  console.log("Overlaying logo...");
  runCommand(
    `ffmpeg -y -i "${finalConcat}" -i "${logoPath}" -filter_complex "overlay=W-w-40:40" -c:a copy "${finalOutput}"`,
    "Logo overlay failed"
  );

  console.log(`Uploading final video to ${outputGcsUri} ...`);
  await uploadFileToGcsUri(finalOutput, outputGcsUri, accessToken);

  console.log("Render complete.");
}

function normalizeVideoVideoOnly(input, output) {
  runCommand(
    `ffmpeg -y -i "${input}" -vf "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2" -r 30 -c:v libx264 -pix_fmt yuv420p -an "${output}"`,
    `normalizeVideoVideoOnly failed for ${input}`
  );
}

function runCommand(command, label) {
  try {
    execSync(command, { stdio: "pipe" });
  } catch (err) {
    const stderr = err.stderr ? err.stderr.toString() : "";
    const stdout = err.stdout ? err.stdout.toString() : "";
    throw new Error(`${label}: ${stderr || stdout || err.message}`);
  }
}

async function downloadDriveFile(link, outPath) {
  const id = extractDriveId(link);
  if (!id) {
    throw new Error(`Could not extract Drive file ID from link: ${link}`);
  }

  const url = `https://drive.google.com/uc?export=download&id=${id}`;
  const res = await fetch(url);

  if (!res.ok) {
    throw new Error(`Failed to download Drive file: HTTP ${res.status} for ${link}`);
  }

  const buffer = Buffer.from(await res.arrayBuffer());
  if (!buffer.length) {
    throw new Error(`Downloaded empty Drive file: ${link}`);
  }

  fs.writeFileSync(outPath, buffer);
}

async function downloadGcsFile(gsUri, outPath, token) {
  const parsed = parseGsUri(gsUri);
  if (!parsed) {
    throw new Error(`Invalid gs:// URI: ${gsUri}`);
  }

  const url = `https://storage.googleapis.com/storage/v1/b/${parsed.bucket}/o/${encodeURIComponent(parsed.object)}?alt=media`;

  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`
    }
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to download GCS file ${gsUri}: HTTP ${res.status} ${text}`);
  }

  const buffer = Buffer.from(await res.arrayBuffer());
  if (!buffer.length) {
    throw new Error(`Downloaded empty GCS file: ${gsUri}`);
  }

  fs.writeFileSync(outPath, buffer);
}

async function uploadFileToGcsUri(localPath, gsUri, token) {
  const parsed = parseGsUri(gsUri);
  if (!parsed) {
    throw new Error(`Invalid output gs:// URI: ${gsUri}`);
  }

  const buffer = fs.readFileSync(localPath);
  const url = `https://storage.googleapis.com/upload/storage/v1/b/${parsed.bucket}/o?uploadType=media&name=${encodeURIComponent(parsed.object)}`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "video/mp4"
    },
    body: buffer
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to upload final video: ${text}`);
  }
}

function parseGsUri(gsUri) {
  const match = String(gsUri).match(/^gs:\/\/([^\/]+)\/(.+)$/);
  if (!match) return null;
  return { bucket: match[1], object: match[2] };
}

function extractDriveId(url) {
  const match = String(url).match(/[-\w]{25,}/);
  return match ? match[0] : null;
}

async function getAccessToken() {
  const res = await fetch(
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
    {
      headers: { "Metadata-Flavor": "Google" }
    }
  );

  if (!res.ok) {
    throw new Error(`Failed to get access token: ${res.status}`);
  }

  const data = await res.json();
  return data.access_token;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
