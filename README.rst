A simple framework for crawlers
===============================

Runtime Features
----------------
* Progress visualization
* Suspend crawling when meet errors or network issues and can continue later on
* Depth first crawling to maximize parallelism
* Parallelization controlled by thread pool to save memory
* Decoupled connector to control connection behavior

Develop Features
----------------
* Assemble crawl logics in a very flexible style like pipelines
* Decouple crawl logics and collected contents
* Debuggability: Automatically collect exceptions during crawling
* Test mode (TODO)
