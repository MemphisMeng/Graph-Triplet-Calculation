EDGES = load 'graph.txt' using PigStorage('\t') as (source: long, dest:long);
EDGES = distinct EDGES;

CANON_EDGES_1 = filter EDGES by source < dest;
CANON_EDGES_2 = filter EDGES by source < dest;

TRIAD_JOIN    = join CANON_EDGES_1 by dest, CANON_EDGES_2 by source;
OPEN_EDGES    = foreach TRIAD_JOIN generate CANON_EDGES_1::source, CANON_EDGES_2::dest;
TRIANGLE_JOIN = join CANON_EDGES_1 by (source,dest), OPEN_EDGES by (CANON_EDGES_1::source, CANON_EDGES_2::dest);
TRIANGLES     = foreach TRIANGLE_JOIN generate 1 as A:int;

CONST_GROUP   = group TRIANGLES all parallel 1;
FINAL_COUNT   = foreach CONST_GROUP generate COUNT(TRIANGLES);

dump FINAL_COUNT;
