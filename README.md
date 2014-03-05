PenE
====

GridEngine accounting metric reporter.

# Definition
Script to make (basic) reports for metrics gathered through GridEngine
accounting.

# Connectors
From the time being there is just one *connector*, consisting in a [SQL 
backend](http://blog.adslweb.net/serendipity/article/270/Load-Grid-Engine-accounting-file-into-MySQL).
The SQL representation of the GridEngine accounting data becomes handy 
when filtering results. The same operations acting directly on the text
file can result in a real pain.

One can whatsoever use the (connector) XML output of `qacct` command, but 
I have found it fairly resource consuming, apart from being more tough to 
filter the results.

# Output (~Formatters)
Uses [pygal](http://pygal.org/).
