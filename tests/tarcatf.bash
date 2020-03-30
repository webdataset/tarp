# Rename info.json to foo extension, discard files with all
# other extensions. Verify that this operates as expected.
fields=foo:info.json
$tarp cat -f $fields testdata.tar -o - | tar tf - | fgrep .foo > /dev/null
$tarp cat -f $fields testdata.tar -o - | tar tf - | fgrep -v .png > /dev/null

