---
sort: 1
---

# The Burroughs Codebase

The Burroughs codebase is broken up into 3 main projects, with some distinct subsections. The basic breakdown is as follows:

- **Core Burroughs Application** (/src)
  - The core classes for query translation
  - Classes for interacting with KsqlDB over REST
  - CLI and utility related classes
  - Unit tests
- **Burroughs Web App** (/SingleMessageTransforms)
  - Burroughs Server (/src)
    - Classes that expose the Burroughs functionality over a REST API
    - REST interface for the postgres database
    - Different logging system
    - Not a lot of code
  - Burroughs React App (/client)
    - Implements the front end for the web interface
- **Single Message Transforms** (/burroughs-server)
  - The code that implements the custom SMTs used by Burroughs. See [Query Execution]({{ '/documentation/execution' | relative_url }}) for more details on what these are for.