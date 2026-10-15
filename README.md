# ValkeyTest


## Setup with docker

We can find all information about Valkey Bundle at https://valkey.io/topics/valkey-bundle/

Valkey Bundle is a pre-packaged, containerized version of Valkey that comes preloaded with a collection of supported modules. These modules enable advanced data structures and search capabilities to extend Valkey’s core functionality. The bundle is designed to help developers quickly get started with powerful Valkey features without needing to manually install or configure anything. Some of the modules included in the bundle are:

- Valkey JSON - Allows users to natively store, query, and modify JSON data structures using the JSONPath query language.
- Valkey Bloom - Provides space-efficient probabilistic data structures, such as Bloom filters, for adding elements and checking whether they exist in a set.
- Valkey Search - Enables the creation of indexes and similarity searches through the use of complex filters.
- Valkey LDAP - Handles user authentication against LDAP based identity providers

## Commands
```bash
wsl
docker pull valkey/valkey-bundle
docker run --name my-valkey-bundle -d -p 6379:6379 valkey/valkey-bundle
docker exec -it my-valkey-bundle valkey-cli -h localhost -p 6379 -3
MODULE LIST
bf, json, search, ldap, lua
exit
```
