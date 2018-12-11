if Code.ensure_loaded?(Ecto) do
  defmodule Honeydew.EctoSource.SQL.Postgres do
    alias Honeydew.EctoSource
    alias Honeydew.EctoSource.SQL

    @behaviour SQL
    @now "(CAST(EXTRACT(epoch FROM NOW()) * 1000 AS BIGINT)"

    @impl true
    def integer_type do
      :bigint
    end

    @impl true
    def ready do
      SQL.far_in_the_past()
      |> time_in_msecs
      |> msecs_ago
    end

    def newreserve(
          repo,
          schema,
          lock_field \\ "honeydew_email_background_jobs_lock",
          key_field \\ "id",
          private_field \\ "honeydew_email_background_jobs_private",
          stale_timeout \\ 1000
        ) do
      import Ecto.Query

      lockfield = String.to_atom(lock_field)
      keyfield = String.to_atom(key_field)

      non_locked_row_subquery =
        schema
        |> select([r], r.id)
        |> where(fragment("? BETWEEN 0 AND ?", ^lock_field, ^msecs_ago(stale_timeout)))
        |> order_by([^lockfield, ^keyfield])
        |> limit(1)
        |> lock("FOR UPDATE SKIP LOCKED")

      query =
        from(r in schema, join: s in subquery(non_locked_row_subquery), on: s.id == r.id)
        |> update([r],
          set:
            ^[
              {lockfield, fragment(@now)}
            ]
        )

      Ecto.Adapters.SQL.to_sql(:update_all, repo, query)
    end

    @impl true
    def reserve(state) do
      "UPDATE #{state.table}
      SET #{state.lock_field} = #{now_msecs()}
      WHERE id = (
        SELECT id
        FROM #{state.table}
        WHERE #{state.lock_field} BETWEEN 0 AND #{msecs_ago(state.stale_timeout)}
        ORDER BY #{state.lock_field}, #{state.key_field}
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING #{state.key_field}, #{state.private_field}"
    end

    @impl true
    def cancel(state) do
      "UPDATE #{state.table}
      SET #{state.lock_field} = NULL
      WHERE
        id = $1
        AND #{state.lock_field} BETWEEN 0 AND #{msecs_ago(state.stale_timeout)}
      RETURNING #{state.lock_field}"
    end

    @impl true
    def status(state) do
      "SELECT COUNT(CASE WHEN #{state.lock_field} IS NOT NULL THEN 1 ELSE NULL END) AS count,
              COUNT(CASE WHEN #{state.lock_field} >= #{msecs_ago(state.stale_timeout)} THEN 1 ELSE NULL END) AS in_progress,
              COUNT(CASE WHEN #{state.lock_field} = #{EctoSource.abandoned()} THEN 1 ELSE NULL END) AS abandoned
      FROM #{state.table}"
    end

    @impl true
    def filter(state, :abandoned) do
      "SELECT id FROM #{state.table} WHERE #{state.lock_field} = #{EctoSource.abandoned()}"
    end

    defp msecs_ago(msecs) do
      "#{now_msecs()} - #{msecs}"
    end

    defp now_msecs do
      "(CAST(EXTRACT(epoch FROM NOW()) * 1000 AS BIGINT))"
    end

    defp time_in_msecs(time) do
      "(CAST(EXTRACT(epoch from timestamp '#{time}') * 1000 AS BIGINT))"
    end
  end
end
