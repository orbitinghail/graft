Next tasks:
- merge control plane and metastore in DESIGN.md
- implement control plane server and client
- connect the page store to the catalog, finish read pages
- consider switching pagestore to websockets or http streaming bodies

# Merge control plane and metastore
Currently DESIGN establishes that the metastore and control plane are separate services. The rational for this was to leverage durable objects to more easily scale the metastore around the world. However, it does add an additional layer of complexity in the implementation.

An easier solution is to merge the metastore into the Control Plane and scale the Control Plane world wide. To do this we will need to shard volume metadata into region-local control planes. We can do this by providing a globally centralized lookup service for locating VolumeIds. This information can be perma-cached in each datacenter, and fly-redirects + smart clients can be used to route meta-traffic to the right location.

To start though, we can just have a single control plane instance and shard all volumes to it. This will be fine for initial deployment.

We are planning on using Neon as the storage layer for the control plane.

I think the only part of the control plane that may need to be centralized is authn/authz. But for now we are just going to use static auth.