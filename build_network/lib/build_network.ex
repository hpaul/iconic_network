require Logger
require Combination
require NimbleCSV

NimbleCSV.define(NetworkParser, separator: ",", escape: ~s(\"))

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

defmodule Network do
  @es_type "re_network"
  @es_index "re_network"
  use Elastic.Document.API

  defstruct source: nil, target: nil, year: nil, weight: nil, citations: nil, articles: nil
end

defmodule BuildNetworkWorker do
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, [])
  end

  def init(_) do
    {:ok, nil}
  end

  def network(server, author_id, co_list) do
    # Don't use cast: http://blog.elixirsips.com/2014/07/16/errata-dont-use-cast-in-a-poolboy-transaction/
    timeout_ms = 1_000_000
    GenServer.call(server, {:network, author_id, co_list}, timeout_ms)
  end

  def handle_call({:network, author_id, co_list}, _from, state) do
    conns = Combination.combine(co_list, 2)
    # Write csv header for nodes
    {:ok, nodes} = File.open("data/#{author_id}_nodes.csv", [:write, :utf8])
    IO.write(nodes, "Id,name,city,country,university\n")
    # Get nodes and write them
    File.close(nodes)

    # Write csv header for edges
    {:ok, edges} = File.open("data/#{author_id}_edges.csv", [:write, :utf8])
    IO.write(edges, "Source,Target,year,weight,citations\n")

    for [source,target] <- conns do
      query = %{
        query: %{
          bool: %{
            must: [
              %{ match: %{ source: source } },
              %{ match: %{ target: target } }
            ]
          }
        }
      }

      list = Network.search(query)

      for edge <- list do
        IO.write(edges, "#{edge.source},#{edge.target},#{edge.weight},#{edge.citations}\n")
      end
    end

    Logger.info " - #{author_id} network finished"
    File.close(edges)
    {:reply, "done", state}
  end
end

defmodule BuildNetwork do
  def list() do
    with {:ok, db} <- Sqlitex.open('../iconic.db'),
         {:ok, data} <- Sqlitex.query(db, "SELECT coauthors.id as id, coauthors.co_list as co_list, author.cited_by_count FROM coauthors INNER JOIN author ON coauthors.id = author.id ORDER BY author.cited_by_count DESC LIMIT 100", db_timeout: 1_000_000)
    do
      {:ok, pool} = :poolboy.start_link(
        [worker_module: BuildNetworkWorker, size: 10, max_overflow: 2]
      )

      for row <- data do
        id = row[:id]
        co_list = Poison.decode!(row[:co_list]) |> Enum.take(2)

        Logger.info "Started process for author #{inspect(id)}"

        :poolboy.transaction(pool, fn(http_requester_pid) ->
          BuildNetworkWorker.network(http_requester_pid, id, co_list)
        end, :infinity)
      end

    else
      {:error, err} -> Logger.error(err)
    end
  end
end

defmodule Network.CLI do

  def main(_args) do
    IO.puts("ICONIC!")
    print_help_message()
    receive_command()
  end

  @commands %{
    "compute" => "Start building authors network"
  }

  defp receive_command do
    IO.gets("\n> ")
    |> String.trim
    |> String.downcase
    |> execute_command
  end

  defp execute_command("compute") do
    compute()
  end

  defp execute_command(_unknown) do
    IO.puts("\nInvalid command. I don't know what to do.")
    print_help_message()

    receive_command()
  end

  defp print_help_message do
    IO.puts("\nYou can run these actions:\n")
    @commands
    |> Enum.map(fn({command, description}) -> IO.puts("  #{command} - #{description}") end)
  end

  def compute() do
    Logger.info "Invoked compute method - Starting process"
    with {:ok, db} <- Sqlitex.open('../iconic.db'),
         {:ok, data} <- Sqlitex.query(db, "SELECT * FROM collaboration", db_timeout: 1_000_000_000),
         {:ok, edges} <- File.open("data/all_edges.csv", [:write, :utf8])
    do
      Logger.info "Data retrived - Start computing"
      IO.write(edges, "Source,Target,year,cumulated_citations,articles\n")

      rows =
        data
        |> Flow.from_enumerable()
        |> Flow.flat_map(fn collaboration ->
          {year, _, _} = collaboration[:published]
          citations = collaboration[:cited_by] || 0
          authors = Poison.decode!(collaboration[:authors])
          ids =
            Enum.reduce(authors, [], fn auth, list ->
              [Map.get(auth, "authid") | list]
            end)
            |> Enum.reject(&is_nil/1)
            |> Enum.map(fn id -> elem(Integer.parse(id), 0) end)
            |> Enum.uniq
            |> Enum.sort
          connections =
            case length(ids) do
              0 -> []
              1 -> []
              _ -> Combination.combine(ids, 2)
            end

          :erlang.garbage_collect()

          Enum.map(connections, fn [source,target] ->
            [source, target, year, citations, collaboration[:abs_id]]
          end)
        end)
        |> Flow.partition()
        |> Flow.reduce(fn -> %{} end, fn [source, target, year, citations, article], acc ->
          Map.update(acc, "#{source}-#{target}-#{year}", [{citations, article}], fn conns ->
            [{citations, article} | conns]
          end)
        end)
        |> Enum.to_list()

      Logger.info "Half the way..."
      :erlang.garbage_collect()

      rows =
        rows
        |> Flow.from_enumerable()
        |> Flow.partition()
        |> Flow.reduce(fn -> [] end, fn {edge, details}, acc ->
          [source, target, year] = String.split(edge, "-")
          citations = Enum.reduce(details, 0, fn {cit, _}, total -> cit + total end)
          articles = Enum.reduce(details, [], fn {_, a}, total -> [a | total] end)

          [[source, target, year, length(details), citations, Enum.join(articles, ",")] | acc]
        end)
        |> Enum.to_list()

      Logger.info "Writing..."
      IO.write(edges, NetworkParser.dump_to_iodata(rows))

      Logger.info "Processed all collaboration, yaaay!!"
    end
  end
end
