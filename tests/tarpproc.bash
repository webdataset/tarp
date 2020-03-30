# For each sample, creates an addition file within that sample using "touch";
# verifies that all the samples get created.
read -r result < <(
    $tarp proc -c 'touch sample.foo' testdata.tar -o - | tar tf - | fgrep .foo | wc -l
)
test $result = 20
