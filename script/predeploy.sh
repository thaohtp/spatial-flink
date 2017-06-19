export BUNDLE_BIN="/jml/apps/Peel/bin"
export BUNDLE_SRC="/jml/data/d/1 CLASS STUDY/1 Lecture/1 IT4BI Second/0 Thesis/3 Lab/spatial-flink/code/spatial-analysis"
export BUNDLE_GID="flink"
export BUNDLE_AID="spatial-analysis"
export BUNDLE_PKG="flink"

export BUNDLE_SRC="/jml/data/d/1 CLASS STUDY/1 Lecture/1 IT4BI Second/3 BDA Project/3 Lab/bdapro_git"
export BUNDLE_GID="de.tu_berlin.dima"
export BUNDLE_AID="bdapro-ws1617"
export BUNDLE_PKG="de.tu_berlin.dima.bdapro.flink"

1. Go to bin folder and create connection to monetdb

sudo usermod -a -G monetdb $USER       # add yourself as MonetDB admin
cd "$BUNDLE_BIN/peel-wordcount"        # go to your bundle root
monetdbd create $(pwd)/results/monetdb # initialize DB farm
monetdbd start $(pwd)/results/monetdb  # start MonetDB with this farm  
monetdb create peel                    # create DB
monetdb release peel                   # release DB maintenance mode
mclient -u monetdb -d peel             # connect to DB (pw:monetdb)

2. Initialize database and import data

./peel.sh db:import wordcount.default --connection=monetdb
./peel.sh db:initialize --connection=monetdb

