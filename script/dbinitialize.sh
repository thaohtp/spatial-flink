echo "Current directory: $(pwd)"
export $CUR_DIR="$(pwd)"

echo "Switch to bundle directory: $BUNDLE_BIN/$BUNDLE_AID"
cd "$BUNDLE_BIN/$BUNDLE_AID"        # go to your bundle root
monetdbd create $(pwd)/results/monetdb # initialize DB farm
monetdbd start $(pwd)/results/monetdb  # start MonetDB with this farm  
monetdb create peel                    # create DB
monetdb release peel                   # release DB maintenance mode
#mclient -u monetdb -d peel             # connect to DB (pw:monetdb)

./peel.sh db:initialize --connection=monetdb

echo "Switch back to current folder: $CUR_DIR"
cd "$CUR_DIR"
