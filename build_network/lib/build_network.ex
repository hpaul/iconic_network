require Logger
require Combination

defmodule Collaborations do
  @es_type "collaborations"
  @es_index "collaborations"
  use Elastic.Document.API

  defstruct id: nil, abs_id: nil, authors: nil, cited_by: nil, keywords: nil, published: nil
end

defmodule Authors do
  @es_type "authors"
  @es_index "authors"
  use Elastic.Document.API

  defstruct auth_id: nil, affiliation_current: nil, full_name: nil
end

defmodule BuildNetwork do

  def network(articles, author_id, co_list) do
    colist_ids = Enum.map(co_list, fn item -> Integer.parse(item.auth_id) |> elem(0) end)

    {:ok, nodes} = File.open("data/#{author_id}_nodes.csv", [:append, :utf8])
    {:ok, edges} = File.open("data/#{author_id}_edges.csv", [:append, :utf8])

    for article <- articles do

      ids =
        Enum.reduce(article.authors, [], fn auth, list ->
          {id, _} = Map.get(auth, "authid") |> Integer.parse
          [id  | list]
        end)
        |> Enum.uniq
        |> Enum.filter(fn id -> Enum.member?(colist_ids, id) end)

      authors =
        ids
        |> Enum.map(fn id -> Enum.find(co_list, fn item -> "#{item.auth_id}" == "#{id}" end) end)

      for auth <- authors do
        name = "#{Map.get(auth.full_name, "initials", "")} #{Map.get(auth.full_name, "surname", "")}"
        city = "#{Map.get(auth.affiliation_current, "affiliation-city", "")}, #{Map.get(auth.affiliation_current, "affiliation-country", "")}"
        univeristy = Map.get(auth.affiliation_current, "affiliation-name", "")

        IO.write(nodes, "#{auth.auth_id};#{name};#{city};#{univeristy}\n")
      end

      connections =
        case length(ids) do
          0 -> []
          1 -> []
          _ -> Combination.combine(ids, 2)
        end

      for [source, target] <- connections do
        IO.write(edges, "#{source};#{target};#{article.published};#{article.cited_by}\n")
      end

    end

    File.close(nodes)
    File.close(edges)
  end

  def coauthors(row) do
    with [author_id, co_list | _] <- row
    do
      list = [author_id | co_list] |> Enum.uniq

      query = %{
        query: %{
          bool: %{
            filter: %{
              terms: %{ auth_id: list }
            }
          }
        },
        size: 10000
      }

      list = Authors.search(query)

      for coauthor <- co_list do
        articles = Collaborations.search(%{
          query: %{
            bool: %{
              filter: %{
                term: %{ "authors.authid": coauthor}
              }
            }
          },
          size: 2000
        })

        Logger.info "Getting articles for #{coauthor} coauthor"
        Task.async(__MODULE__, :network, [articles, author_id, list])
      end
      |> Enum.map(fn task -> Task.await(task, 1_000_000) end)

    else
      {:error, _} -> {}
    end
  end

  def list() do
    with {:ok, db} <- Mariaex.start_link(
                        username: "root",
                        password: "H@mst3rdigital",
                        database: "iconic",
                        show_sensitive_data_on_connection_error: true
                      )
    do
      {:ok, data} = Mariaex.query(db, "SELECT coauthors.id as id, coauthors.co_list as co_list, author.cited_by_count FROM coauthors INNER JOIN author ON coauthors.id = author.id ORDER BY author.cited_by_count DESC LIMIT 50 OFFSET 50")

      for row <- data.rows do
        id = List.first(row)
        # Write csv header for nodes
        {:ok, nodes} = File.open("data/#{id}_nodes.csv", [:write, :utf8])
        IO.write(nodes, "Id;name;country;university\n")
        File.close(nodes)

        # Write csv header for edges
        {:ok, edges} = File.open("data/#{id}_edges.csv", [:write, :utf8])
        IO.write(edges, "Source;Target;year;citations\n")
        File.close(edges)

        Logger.info "Started process for author #{inspect(id)}"
        Task.start(__MODULE__, :coauthors, [row])
      end

    else
      {:error, err} -> err
    end
  end
end
