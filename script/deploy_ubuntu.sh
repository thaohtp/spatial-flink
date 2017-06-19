echo "Current directory: $(pwd)"
export CUR_DIR="$(pwd)"

echo "Setting environment variables"
export BASE_DIR="/home/parallels/mac"
export PROJ_DIR="$BASE_DIR/proj"

export MAC_BUNDLE_BIN="/jml/apps/Peel/bin"
export MAC_BUNDLE_SRC="/jml/data/d/1 CLASS STUDY/1 Lecture/1 IT4BI Second/0 Thesis/3 Lab/spatial-flink/code/spatial-analysis"
export BUNDLE_GID="de.tu_berlin.dima"
export BUNDLE_AID="spatial-analysis"
export BUNDLE_PKG="de.tu_berlin.dima"
export BUNDLE_BIN="$BASE_DIR/apps/Peel/bin"
export BUNDLE_SRC="$PROJ_DIR/$BUNDLE_AID"

echo "Switch to project directory: $PROJ_DIR"
cd "$PROJ_DIR"

echo "Delete old code in $PROJ_DIR"
rm -R -f "$PROJ_DIR/$BUNDLE_AID"

echo "Copy new code from Mac to $PROJ_DIR"
cp -R "$MAC_BUNDLE_SRC" "$PROJ_DIR"

echo "Delete old bin directory in $BUNDLE_BIN"
rm -R "$BUNDLE_BIN/$BUNDLE_AID"

echo "Deploy project by copying from Mac-Peel bin to $BUNDLE_BIN"
cp -R "$MAC_BUNDLE_BIN/$BUNDLE_AID" "$BUNDLE_BIN"

echo "Switch back to current directory"
cd "$CUR_DIR"
