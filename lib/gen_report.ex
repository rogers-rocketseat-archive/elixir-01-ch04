defmodule GenReport do
  alias GenReport.Parser

  @names [
    "cleiton",
    "daniele",
    "danilo",
    "diego",
    "giuliano",
    "jakeliny",
    "joseph",
    "mayk",
    "rafael",
    "vinicius"
  ]

  @months [
    "abril",
    "agosto",
    "dezembro",
    "fevereiro",
    "janeiro",
    "julho",
    "junho",
    "maio",
    "março",
    "novembro",
    "outubro",
    "setembro"
  ]

  @years [
    2016,
    2017,
    2018,
    2019,
    2020
  ]

  def build(filename) do
    filename
    |> Parser.parse_file()
    |> Enum.reduce(
      get_accumulator_struct(),
      fn line, accumulator -> merge_hours(line, accumulator) end
    )
  end

  def build_from_many(filenames) do
    result =
      filenames
      # |> Task.async_stream(fn filename -> build(filename) end)
      |> Task.async_stream(&build/1)
      |> Enum.reduce(
        get_accumulator_struct(),
        fn {:ok, result}, report -> sum_hours(report, result) end
      )

    {:ok, result}
  end

  def build() do
    {:error, "Insira o nome de um arquivo"}
  end

  defp merge_hours(line, accumulator) do
    %{
      "all_hours" => all_hours,
      "hours_per_month" => hours_per_month,
      "hours_per_year" => hours_per_year
    } = accumulator

    %{
      accumulator
      | "all_hours" => get_hours(line, all_hours),
        "hours_per_month" => get_hours_per_month(line, hours_per_month),
        "hours_per_year" => get_hours_per_year(line, hours_per_year)
    }
  end

  defp sum_hours(map1, map2) do
    %{
      "all_hours" => all_hours1,
      "hours_per_month" => hours_per_month1,
      "hours_per_year" => hours_per_year1
    } = map1

    %{
      "all_hours" => all_hours2,
      "hours_per_month" => hours_per_month2,
      "hours_per_year" => hours_per_year2
    } = map2

    all_hours = merge_maps(all_hours1, all_hours2)
    hours_per_month = merge_maps(hours_per_month1, hours_per_month2)
    hours_per_year = merge_maps(hours_per_year1, hours_per_year2)

    %{
      "all_hours" => all_hours,
      "hours_per_month" => hours_per_month,
      "hours_per_year" => hours_per_year
    }
  end

  defp merge_maps(map1, map2) do
    Map.merge(map1, map2, &deep_resolve/3)
  end

  defp deep_resolve(_key, left = %{}, right = %{}) do
    merge_maps(left, right)
  end

  defp deep_resolve(_key, left, right) do
    left + right
  end

  defp get_hours(line, all_hours_map) do
    [name, hours, _day, _month, _year] = line

    Map.put(
      all_hours_map,
      name,
      all_hours_map[name] + hours
    )
  end

  defp get_hours_per_month(line, hours_per_month_map) do
    [name, hours, _day, month, _year] = line
    months_map = hours_per_month_map[name]

    new_months_map =
      Map.put(
        months_map,
        month,
        months_map[month] + hours
      )

    Map.put(
      hours_per_month_map,
      name,
      new_months_map
    )
  end

  defp get_hours_per_year(line, hours_per_year_map) do
    [name, hours, _day, _month, year] = line
    years_map = hours_per_year_map[name]

    new_years_map =
      Map.put(
        years_map,
        year,
        years_map[year] + hours
      )

    Map.put(
      hours_per_year_map,
      name,
      new_years_map
    )
  end

  defp get_accumulator_struct() do
    all_hours = Enum.into(@names, %{}, fn x -> {x, 0} end)

    months = Enum.into(@months, %{}, fn x -> {x, 0} end)
    hours_per_month = Enum.into(@names, %{}, fn x -> {x, months} end)

    years = Enum.into(@years, %{}, fn x -> {x, 0} end)
    hours_per_year = Enum.into(@names, %{}, fn x -> {x, years} end)

    %{
      "all_hours" => all_hours,
      "hours_per_month" => hours_per_month,
      "hours_per_year" => hours_per_year
    }
  end
end
