# Test concatenation of two .tar files with tarp cat;
# select only the files with .png extension in each record.
# Make sure the result contains twice 20 records.
read -r result < <(
    $tarp cat -f png testdata.tar testdata.tar -o - | tar tf - | wc -l
)
test $result = 40

