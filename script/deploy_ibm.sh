echo "Setting environment variable"
export BUNDLE_BIN="/jml/apps/Peel/bin"
export BUNDLE_SRC="/jml/data/d/1 CLASS STUDY/1 Lecture/1 IT4BI Second/0 Thesis/3 Lab/spatial-flink/code/spatial-analysis"
export BUNDLE_GID="de.tu_berlin.dima"
export BUNDLE_AID="spatial-analysis"
export BUNDLE_PKG="de.tu_berlin.dima"
export SERVER_BIN="/home/hadoop/thaohtp/peel"

echo "Current folder: $(pwd)"
export CUR_FOLDER="$(pwd)"

#echo "Switch to $BUNDLE_SRC"
#cd "$BUNDLE_SRC"

echo "Clean $BUNDLE_AID.zip"
rm -R "$BUNDLE_BIN/$BUNDLE_AID.zip"

echo "Compress file to send to server"
zip -r $BUNDLE_AID.zip $BUNDLE_BIN/$BUNDLE_AID

echo "Copy to server"
scp $BUNDLE_BIN/$BUNDLE_AID.zip thaohtp@@ibm-power-1.dima.tu-berlin.de:$SERVER_BIN

echo "Switch back to current folder"
cd "$CUR_FOLDER"
