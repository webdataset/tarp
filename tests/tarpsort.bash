# Verify that sorting files does not lose any files.
read -r result < <(
    $tarp sort -f info.json -s __key__ testdata.tar -o - | tee _out.tar | tar tf - | wc -l
)
sleep 1
echo result = $result
test $result = 20

