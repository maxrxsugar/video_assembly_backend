const fs = require("fs");
const os = require("os");
const path = require("path");
const { execSync } = require("child_process");
const { Storage } = require("@google-cloud/storage");

const storage = new Storage();

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

  const middleConcatInput = path.join(workDir, "middle_concat_input.txt");
  const middleConcat = path.join(workDir, "middle_concat.mp4");
  const middleNarr = path.join(workDir, "middle_with_narration.mp4");
  const middleExtended = path.join(workDir, "middle_extended.mp4");
  const freezeClip = path.join(workDir, "freeze_tail.mp4");
  const extendConcatInput = path.join(workDir, "extend_concat_input.txt");

  const finalConcat = path.join(workDir, "final_concat.mp4");
  const finalOutput = path.join(workDir, "final_output.mp4");

  console.log("Downloading static assets...");
  await downloadDriveFile(introBumperLink, introPath);
  await downloadDriveFile(outroBumperLink, outroPath);
  await downloadDriveFile(logoOverlayLink, logoPath);

  console.log("Downloading generated assets...");
  await downloadGcsFile(hookClipUrl, hookPath, accessToken);
  await downloadGcsFile(scienceClipUrl, sciencePath, accessToken);
  await downloadGcsFile(productClipUrl, productPath, accessToken);
  await downloadGcsFile(narrationAudioUrl, narrationPath, accessToken);

  probeFile(introPath, "intro original");
  probeFile(outroPath, "outro original");
  probeFile(narrationPath, "narration original");
  probeFile(hookPath, "hook original");
  probeFile(sciencePath, "science original");
  probeFile(productPath, "product original");

  console.log("Normalizing Veo clips as video-only...");
  normalizeVideoOnly(hookPath, hookNorm);
  normalizeVideoOnly(sciencePath, scienceNorm);
  normalizeVideoOnly(productPath, productNorm);

  console.log("Normalizing intro/outro with preserved audio...");
  normalizeVideoWithAudio(introPath, introNorm);
  normalizeVideoWithAudio(outroPath, outroNorm);

  probeFile(introNorm, "intro normalized");
  probeFile(outroNorm, "outro normalized");
  probeFile(hookNorm, "hook normalized");
  probeFile(scienceNorm, "science normalized");
  probeFile(productNorm, "product normalized");

  console.log("Concatenating middle clips...");
  fs.writeFileSync(
    middleConcatInput,
    [
      `file '${hookNorm}'`,
      `file '${scienceNorm}'`,
      `file '${productNorm}'`
    ].join("\n")
  );

  runCommand(
    `ffmpeg -y -f concat -safe 0 -i "${middleConcatInput}" -c:v libx264 -pix_fmt yuv420p -an "${middleConcat}"`,
    "Concatenating middle clips failed"
  );

  probeFile(middleConcat, "middle concatenated video-only");

  console.log("Checking durations...");
  const narrationDuration = getDuration(narrationPath);
  const middleDuration = getDuration(middleConcat);

  console.log(`Narration duration: ${narrationDuration}`);
  console.log(`Middle duration: ${middleDuration}`);

  let sourceForNarration = middleConcat;

  if (narrationDuration > middleDuration) {
    const freezeDuration = narrationDuration - middleDuration;
    console.log(`Extending middle with freeze frame by ${freezeDuration} seconds...`);

    runCommand(
      `ffmpeg -y -sseof -0.1 -i "${middleConcat}" -vf "tpad=stop_mode=clone:stop_duration=${freezeDuration},fps=30,format=yuv420p" -c:v libx264 -an "${freezeClip}"`,
      "Freeze frame generation failed"
    );

    fs.writeFileSync(
      extendConcatInput,
      [
        `file '${middleConcat}'`,
        `file '${freezeClip}'`
      ].join("\n")
    );

    runCommand(
      `ffmpeg -y -f concat -safe 0 -i "${extendConcatInput}" -c:v libx264 -pix_fmt yuv420p -an "${middleExtended}"`,
      "Extending middle video failed"
    );

    sourceForNarration = middleExtended;
  }

  probeFile(sourceForNarration, "middle source before narration");

  console.log("Adding narration...");
  runCommand(
    `ffmpeg -y -i "${sourceForNarration}" -i "${narrationPath}" -map 0:v:0 -map 1:a:0 -vf "fps=30,format=yuv420p" -c:v libx264 -c:a aac -b:a 192k -ar 48000 -ac 2 -shortest "${middleNarr}"`,
    "Adding narration to middle failed"
  );

  probeFile(middleNarr, "middle with narration");

  console.log("Concatenating intro + middle + outro with filter_complex...");
  runCommand(
    `ffmpeg -y -i "${introNorm}" -i "${middleNarr}" -i "${outroNorm}" -filter_complex "[0:v][0:a][1:v][1:a][2:v][2:a]concat=n=3:v=1:a=1[outv][outa]" -map "[outv]" -map "[outa]" -c:v libx264 -pix_fmt yuv420p -c:a aac -b:a 192k -ar 48000 -ac 2 "${finalConcat}"`,
    "Final concat with audio failed"
  );

  probeFile(finalConcat, "final concatenated before logo");

  console.log("Overlaying logo...");
  runCommand(
    `ffmpeg -y -i "${finalConcat}" -i "${logoPath}" -filter_complex "[1:v]scale=-1:10[logo];[0:v][logo]overlay=W-w-20:20" -map 0:v:0 -map 0:a:0 -c:v libx264 -pix_fmt yuv420p -c:a aac -b:a 192k -ar 48000 -ac 2 "${finalOutput}"`,
    "Logo overlay failed"
  );

  probeFile(finalOutput, "final output");

  console.log(`Uploading final video to ${outputGcsUri} ...`);
  await uploadFileToGcsUri(finalOutput, outputGcsUri);

  console.log("Render complete.");
}

function normalizeVideoOnly(input, output) {
  runCommand(
    `ffmpeg -y -i "${input}" -vf "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,fps=30,format=yuv420p" -c:v libx264 -an "${output}"`,
    `normalizeVideoOnly failed for ${input}`
  );
}

function normalizeVideoWithAudio(input, output) {
  runCommand(
    `ffmpeg -y -i "${input}" -vf "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,fps=30,format=yuv420p" -c:v libx264 -c:a aac -b:a 192k -ar 48000 -ac 2 "${output}"`,
    `normalizeVideoWithAudio failed for ${input}`
  );
}

function getDuration(filePath) {
  const output = execSync(
    `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${filePath}"`,
    { stdio: "pipe" }
  ).toString().trim();

  return parseFloat(output);
}

function probeFile(filePath, label) {
  try {
    const output = execSync(
      `ffprobe -v error -show_entries stream=index,codec_type,codec_name,channels,sample_rate:format=duration -of compact=p=0:nk=0 "${filePath}"`,
      { stdio: "pipe" }
    ).toString().trim();

    console.log(`FFPROBE ${label}: ${output}`);
  } catch (err) {
    console.log(`FFPROBE FAILED for ${label}: ${err.message}`);
  }
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

async function uploadFileToGcsUri(localPath, gsUri) {
  const parsed = parseGsUri(gsUri);
  if (!parsed) {
    throw new Error(`Invalid output gs:// URI: ${gsUri}`);
  }

  await storage.bucket(parsed.bucket).upload(localPath, {
    destination: parsed.object,
    resumable: true,
    metadata: {
      contentType: "video/mp4"
    }
  });
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
