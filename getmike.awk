BEGIN {
    OFS = "," 
    print "time,host,cores,MEMTOT,MEMUSE,queue,used,drained"
}

# Print previous node, with blanks for missing queue, if needed
function flush_node() {
    if (have_node && !have_queue) {
        print t, h, c, m1, m2, "", "", ""
    }
    have_queue = 0
}

$3 == "linux-x64" {
    flush_node()
    t  = $1
    h  = $2
    c  = $4
    m1 = $6
    m2 = $7
    have_node = 1
}

$3 == "BIP" && $4 ~ /^[0-9]+\/[0-9]+\/[0-9]+$/ {
    split($4, s, "/")
    print t, h, c, m1, m2, $2, s[2], $5
    have_queue = 1
}

END {
    # After last line, if last node had no queue, print it!
    flush_node()
}