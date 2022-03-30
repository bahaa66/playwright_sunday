# CogyntWorkstationIngest
 
To start your Phoenix server:
  * Install dependencies with `mix deps.get`
  * Create and migrate your database with `mix ecto.setup`
  * Start Phoenix endpoint with `mix phx.server`
Now you can visit [`localhost:4002`](http://localhost:4002) from your browser.

Ready to run in production? Please [check our deployment guides](https://hexdocs.pm/phoenix/deployment.html).

## Deps
These are the list of services that you will need to have running in order to run cogynt-ws-ingest:

- Redis
- Kafka
- Zookeeper
- ELasticsearch
- Postgresql

## Elasticsearch

Elasticseach index setting and mapping configurations are stored as json files in the `priv/elasticsearch` directory. The directory
contains one active (per env) and many archived configuration json files. The active file is read by the application at start up
and compared to the index on the server to check for differences and reindex if needed. There is a `mix elasticsearch.update_index <INDEX NAME>` task you can run when you need
to make modifications to an index:

`mix elasticsearch.update_index --env dev event`

In the example above the task will check for an active configuration file, creates an archived file, copies contents of the active file to the archive, and then updates the active
file name with the new time string. You can then make changes to the active file and the application will pick up on the changes on the next start up and trigger a reindex. The
optional `--env` argument tells it what env you are targeting ie dev or prod. If you don't provide an env it will default to dev. If an active file does not exist if will create
one with defaults dependent upon the env.

## Learn more

  * Official website: http://www.phoenixframework.org/
  * Guides: https://hexdocs.pm/phoenix/overview.html
  * Docs: https://hexdocs.pm/phoenix
  * Mailing list: http://groups.google.com/group/phoenix-talk
  * Source: https://github.com/phoenixframework/phoenix
 
