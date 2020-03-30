# Verify that sorting files does not lose any files.
read -r result < <(
    $tarp sort -f png testdata.tar -o - | tar tf - | wc -l
)
test $result = 20

