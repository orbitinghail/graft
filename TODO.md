Next step is to investigate an alternative to on disk hash tables. In my haste to write up the design doc I failed to consider that we need to search segments for the latest version of a given offset... hence we need fast prefix scan.

Neon uses b-tree's, but a radix/prefix tree may also work well. We care about simplicity and read performance and size. Our indexes are completely immutable.