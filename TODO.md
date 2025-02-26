Stack:

- sqlite antithesis workload
- garbage collection
- consider switching pagestore to websockets or http streaming bodies
- authentication (api keys)

# Variable sized pages idea

I am very curious how much impact variable sized pages would be to Graft adoption. Currently pages are exactly 4KiB which will likely limit workloads. We could implement variable length pages in one of two ways:

1. Each page is variable. This is the most flexible option, allowing Graft to be used to replicate lists of things for example.

2. Each Volume's page size can be configured at creation. This is less flexible as it still restricts use cases, however it is more flexible than the current setup. It would likely allow Graft more optimization flexibility in storage.

The primary downside of either approach is complexity. It also starts to beg the question of whether Graft should just offer a non-paged abstraction layer that internally maps onto pages.

I'm leaning towards building (1) as it feels like a reasonable lift from the current design.
