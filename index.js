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

    const hookClipUrl = data.hook_clip_url;
    const scienceClipUrl = data.science_clip_url;
    const productClipUrl = data.product_clip_url;
    const narrationAudioUrl = data.narration_audio_url;

    const introBumperLink = data.intro_bumper;
    const outroBumperLink = data.outro_bumper;
    const logoOverlayLink = data.logo_overlay;

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

    const middleConcat = path.join(workDir, "middle.mp4");
    const middleNarr = path.join(workDir, "middle_narr.mp4");

    const introNorm = path.join(workDir, "intro_norm.mp4");
    const outroNorm = path.join(workDir, "outro_norm.mp4");

    const finalConcat = path.join(workDir, "final_no_logo.mp4");
    const finalOutput = path.join(workDir, "final_output.mp4");

    const concatFile = path.join(workDir, "concat.txt");
    const finalConcatFile = path.join(workDir, "final_concat.txt");

    await downloadDriveFile(introBumperLink, introPath);
    await downloadDriveFile(outroBumperLink, outroPath);
    await downloadDriveFile(logoOverlayLink, logoPath);

    await downloadGcsFile(hookClipUrl, hookPath, accessToken);
    await downloadGcsFile(scienceClipUrl, sciencePath, accessToken);
    await downloadGcsFile(productClipUrl, productPath, accessToken);
    await downloadGcsFile(narrationAudioUrl, narrationPath, accessToken);

    normalizeVideo(hookPath, hookNorm);
    normalizeVideo(sciencePath, scienceNorm);
    normalizeVideo(productPath, productNorm);

    fs.writeFileSync(
      concatFile,
      [
        `file '${hookNorm}'`,
        `file '${scienceNorm}'`,
        `file '${productNorm}'`
      ].join("\n")
    );

    execSync(
      `ffmpeg -y -f concat -safe 0 -i "${concatFile}" -c:v libx264 -pix_fmt yuv420p -c:a aac "${middleConcat}"`
    );

    execSync(
      `ffmpeg -y -i "${middleConcat}" -i "${narrationPath}" -map 0:v -map 1:a -c:v copy -c:a aac -shortest "${middleNarr}"`
    );

    normalizeVideo(introPath, introNorm);
    normalizeVideo(outroPath, outroNorm);

    fs.writeFileSync(
      finalConcatFile,
      [
        `file '${introNorm}'`,
        `file '${middleNarr}'`,
        `file '${outroNorm}'`
      ].join("\n")
    );

    execSync(
      `ffmpeg -y -f concat -safe 0 -i "${finalConcatFile}" -c:v libx264 -pix_fmt yuv420p -c:a aac "${finalConcat}"`
    );

    execSync(
      `ffmpeg -y -i "${finalConcat}" -i "${logoPath}" -filter_complex "overlay=W-w-40:40" -c:a copy "${finalOutput}"`
    );

    const safeName = productName
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_");

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

function normalizeVideo(input, output) {
  execSync(
    `ffmpeg -y -i "${input}" -vf "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2" -r 30 -c:v libx264 -pix_fmt yuv420p -c:a aac "${output}"`
  );
}

async function downloadDriveFile(link, outPath) {
  const id = extractDriveId(link);
  const url = `https://drive.google.com/uc?export=download&id=${id}`;

  const res = await fetch(url);
  const buffer = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(outPath, buffer);
}

async function downloadGcsFile(gsUri, outPath, token) {
  const match = gsUri.match(/^gs:\/\/([^\/]+)\/(.+)$/);
  const bucket = match[1];
  const object = match[2];

  const url = `https://storage.googleapis.com/storage/v1/b/${bucket}/o/${encodeURIComponent(object)}?alt=media`;

  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`
    }
  });

  const buffer = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(outPath, buffer);
}

async function uploadFileToGcs(localPath, bucket, object, token) {
  const buffer = fs.readFileSync(localPath);

  const url =
    `https://storage.googleapis.com/upload/storage/v1/b/${bucket}/o?uploadType=media&name=${encodeURIComponent(object)}`;

  await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "video/mp4"
    },
    body: buffer
  });
}

function extractDriveId(url) {
  const match = url.match(/[-\w]{25,}/);
  return match ? match[0] : null;
}

async function getAccessToken() {
  const res = await fetch(
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
    { headers: { "Metadata-Flavor": "Google" } }
  );

  const data = await res.json();
  return data.access_token;
}
