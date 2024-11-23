Upcoming:
- consider switching pagestore to websockets or http streaming bodies
- end to end testing framework

# Metrics/Monitoring

After a lot of writing code, I discovered that fly.io provides tons of out of the box metrics including http, system, etc. So... removed all the http metrics code, but keeping around the registry and handler.

Remaining tasks for metrics:
- basic metrics for tasks (e.g. writer/uploader and perhaps general metrics for the supervisor)
- perhaps a simple grafana dashboard

To make metrics on supervised tasks a bit more ergonomic, it would be nice to generalize them on the SupervisedTask trait in some way... However it's quite difficult to make a general version of the Registry which makes things tricky. Need to think a bit more about this.