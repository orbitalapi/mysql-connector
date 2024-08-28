# MySQL plugin

This repo contains a MySQL plugin for [Orbital](https://orbitalhq.com)

## Loading the plugin

### plugins.conf

To load the plugin, you first need to provide a `plugins.conf` file to Orbital.

Here's a sample:

```hocon
plugins: [
    { url: "https//repo.orbitalhq.com/release/com/orbitalhq/plugins/connectors/mysql-connector/mysql-connector-0.34.0.jar"}
]
```

This file is downloaded at runtime. You may choose to self-host this file within your own infrastructure
to avoid a runtime dependency on Orbital's repository.

 ### Specifying a plugins.conf
When Orbital starts, it checks the following locations by default for a `plugins.conf` file:

 - `plugins.conf` (relative to the path where Orbital was started)
 - `~./orbital/plugins.conf`

You can specify a custom path by specifying the `--vyne.plugins.path` parameter at startup. Eg:

```
--vyne.plugins.path=/opt/oribtal/plugins.conf
```

Alternatively, loading via a URL:

```
--vyne.plugins.path=https://myserver.com/some-plugins.conf
```
