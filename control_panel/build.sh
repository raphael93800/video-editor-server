#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_NAME="VideoPipeline"
APP_DIR="$SCRIPT_DIR/dist/$APP_NAME.app"
MACOS_DIR="$APP_DIR/Contents/MacOS"
RESOURCES_DIR="$APP_DIR/Contents/Resources"

echo "=== Building $APP_NAME.app ==="

rm -rf "$SCRIPT_DIR/dist"
mkdir -p "$MACOS_DIR" "$RESOURCES_DIR"

cp "$SCRIPT_DIR/app.py" "$RESOURCES_DIR/"
cp "$SCRIPT_DIR/config.py" "$RESOURCES_DIR/"

PYTHON_PATH=$(which python3)

cat > "$MACOS_DIR/$APP_NAME" << LAUNCHER
#!/bin/bash
cd "\$(dirname "\$0")/../Resources"
exec "$PYTHON_PATH" app.py
LAUNCHER
chmod +x "$MACOS_DIR/$APP_NAME"

cat > "$APP_DIR/Contents/Info.plist" << 'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>VideoPipeline</string>
    <key>CFBundleDisplayName</key>
    <string>Video Pipeline</string>
    <key>CFBundleIdentifier</key>
    <string>com.videopipeline.panel</string>
    <key>CFBundleVersion</key>
    <string>1.0.1</string>
    <key>CFBundleShortVersionString</key>
    <string>1.0.1</string>
    <key>CFBundleExecutable</key>
    <string>VideoPipeline</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>NSHighResolutionCapable</key>
    <true/>
</dict>
</plist>
PLIST

echo ""
echo "=== Done! ==="
echo "App: $APP_DIR"
