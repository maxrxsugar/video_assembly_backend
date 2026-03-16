const functions = require("@google-cloud/functions-framework");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { execSync } = require("child_process");

functions.http("helloHttp", async (req, res) => {
  try {
    const data = req.body || {};

    if (data.action !== "assemble_final_video") {
      return res.status(400).json({ error: "Unsupported action." });
    }

    const productName = data.product_name || "product";

    const hookClipUrl = data.hook_clip_url || "";
    const scienceClipUrl = data.science_clip_url || "";
    const productClipUrl = data.product_clip_url || "";
    const narrationAudioUrl = data.narration_audio_url || "";

    const introBumperLink = data.intro_bumper || "";
    const outroBumperLink = data.outro_bumper || "";
    const logoOverlayLink = data.logo_overlay || "";

    if (!hookClipUrl || !scienceClipUrl || !productClipUrl || !narrationAudioUrl) {
      return res.status(400).json({
        error: "Missing one or more clip/audio URLs"
      });
    }

    if (!introBumperLink || !outroBumperLink || !logoOverlayLink) {
      return res.status(400).json({
        error: "Missing CONFIG asset links"
      });
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

    // Download static assets from Drive
    await downloadDriveFile(introBumperLink, introPath);
    await downloadDriveFile(outroBumperLink, outroPath);
    await downloadDriveFile(logoOverlayLink, logoPath);

    // Download generated assets from GCS
    await downloadGcsFile(hookClipUrl, hookPath, accessToken);
    await downloadGcsFile(scienceClipUrl, sciencePath, accessToken);
    await downloadGcsFile(productClipUrl, productPath, accessToken);
    await downloadGcsFile(narrationAudioUrl, narrationPath, accessToken);

    // Normalize videos and STRIP audio from AI-generated clips
    normalizeVideoVideoOnly(hookPath, hookNorm);
    normalizeVideoVideoOnly(sciencePath, scienceNorm);
    normalizeVideoVideoOnly(productPath, productNorm);

    // Also normalize intro/outro without audio for a cleaner first-pass assembly
    normalizeVideoVideoOnly(introPath, introNorm);
    normalizeVideoVideoOnly(outroPath, outroNorm);

    // Concatenate middle clips
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

    // Add narration as the only audio for the middle section
    runCommand(
      `ffmpeg -y -i "${middleConcat}" -i "${narrationPath}" -map 0:v:0 -map 1:a:0 -c:v copy -c:a aac -shortest "${middleNarr}"`,
      "Muxing narration onto middle section failed"
    );

    // Concatenate intro + middle + outro
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

    // Overlay logo top-right
    runCommand(
      `ffmpeg -y -i "${finalConcat}" -i "${logoPath}" -filter_complex "overlay=W-w-40:40" -c:a copy "${finalOutput}"`,
      "Logo overlay failed"
    );

    const safeName = String(productName)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "");

    const fileName = `${safeName}_${Date.now()}.mp4`;
    const bucket = "video_automation_assets";
    const object = `final_videos/${fileName}`;

    await uploadFileToGcs(finalOutput, bucket, object, accessToken);

    return res.json({
      final_video_url: `gs://${bucket}/${object}`,
      render_status: "final_video_done"
    });
  } catch (err) {
    return res.status(500).json({
      error: "Assembly failed",
      details: err.message
    });
  }
});

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
  const match = String(gsUri).match(/^gs:\/\/([^\/]+)\/(.+)$/);
  if (!match) {
    throw new Error(`Invalid gs:// URI: ${gsUri}`);
  }

  const bucket = match[1];
  const object = match[2];

  const url = `https://storage.googleapis.com/storage/v1/b/${bucket}/o/${encodeURIComponent(object)}?alt=media`;

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

async function uploadFileToGcs(localPath, bucket, object, token) {
  const buffer = fs.readFileSync(localPath);

  const url = `https://storage.googleapis.com/upload/storage/v1/b/${bucket}/o?uploadType=media&name=${encodeURIComponent(object)}`;

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
