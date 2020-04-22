# For each sample, creates an addition file within that sample using "touch";
# verifies that all the samples get created.
read -r result < <(
    $tarp proc -c 'touch sample.foo 1>&2' testdata.tar -o - |
        tee _output.tar | tar tf - | fgrep .foo | wc -l
)
test $result = 20
