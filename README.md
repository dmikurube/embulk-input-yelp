# Yelp input plugin for Embulk

Search for Yelp businesses with the [API v3](https://www.yelp.com/developers/documentation/v3/business_search).

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **access_token**: Yelp OAuth2 access token (string, required)
- **location**: Location (string, required)
- **maximum_retries**: Maximum number of retries (integer, default: 7)
- **initial_retry_interval_millis**: Initial interval between retries in milliseconds (integer, default: 1000)
- **maximum_retry_interval_millis**: Maximum interval between retries in milliseconds (integer, default: 60000)

## Example

```yaml
in:
  type: yelp
  access_token: ********
  location: Kyoto
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
