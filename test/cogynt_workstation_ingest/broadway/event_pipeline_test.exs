defmodule CogyntWorkstationIngest.Test.Broadway.EventPipelineTest do
  use CogyntWorkstationIngest.DataCase

  alias CogyntWorkstationIngest.Repo
  alias Models.Events.{EventDefinition, EventDefinitionDetail, Event, EventDetail}
  alias Models.Notifications.{NotificationSetting, Notification}
  alias Models.System.SystemTag

  @event_definition %{
    active: true,
    authoring_event_definition_id: "3171f670-f4fc-11e9-a42c-8c85900cbd75",
    created_at: "2020-02-24T19:13:35Z",
    deleted_at: nil,
    event_type: "entity",
    fields: %{
      "atm_id" => "string",
      "atm_name" => "string",
      "city" => "string",
      "coordinate" => "geo",
      "district" => "string",
      "state" => "string",
      "zip_code" => "integer"
    },
    id: "b61dbd12-5918-4fe9-ab55-a2c48551c167",
    primary_title_attribute: nil,
    title: "atm_locations_entity",
    topic: "atm_locations_entities",
    updated_at: "2020-02-24T19:13:35Z"
  }

  @event_messages [
    %{
      event: %{
        "$crud" => "create",
        "atm_id" => "atm326",
        "atm_name" => "Bank of America",
        "city" => "Livermore",
        "coordinate" => %{
          "geometry" => %{
            "coordinates" => [-121.77501219999999, 37.6821153],
            "type" => "Point"
          },
          "properties" => %{"name" => "Point"},
          "type" => "Feature"
        },
        "data_type" => "atm_locations_entity",
        "district" => "Trevarno",
        "id" => "3ed7f8de-11f0-33d7-a7ff-3cbde8d82e9c",
        "latency" => 171,
        "published_at" => "2019-10-22T20:50:23.053Z",
        "published_by" => "3ed7f8de-11f0-33d7-a7ff-3cbde8d82e9c",
        "publishing_template_type" => "516bccbe-f4ff-11e9-b63b-8c85900cbd75",
        "publishing_template_type_name" => "create_location_entities",
        "source" => "3171f670-f4fc-11e9-a42c-8c85900cbd75",
        "state" => "California",
        "zip_code" => 94550
      },
      event_definition: @event_definition,
      retry_count: 0
    },
    %{
      event: %{
        "$crud" => "create",
        "atm_id" => "atm348",
        "atm_name" => "ID_2204100048",
        "city" => "Sunnyvale",
        "coordinate" => %{
          "geometry" => %{
            "coordinates" => [-122.0500871, 37.3517401],
            "type" => "Point"
          },
          "properties" => %{"name" => "Point"},
          "type" => "Feature"
        },
        "data_type" => "atm_locations_entity",
        "district" => "Serra",
        "id" => "cd48a5c1-f2b2-3795-a886-aa89b5407b58",
        "latency" => 171,
        "published_at" => "2019-10-22T20:50:23.055Z",
        "published_by" => "cd48a5c1-f2b2-3795-a886-aa89b5407b58",
        "publishing_template_type" => "516bccbe-f4ff-11e9-b63b-8c85900cbd75",
        "publishing_template_type_name" => "create_location_entities",
        "source" => "3171f670-f4fc-11e9-a42c-8c85900cbd75",
        "state" => "California",
        "zip_code" => 94087
      },
      event_definition: @event_definition,
      retry_count: 0
    },
    %{
      event: %{
        "$crud" => "create",
        "atm_id" => "atm397",
        "atm_name" => "Chase",
        "city" => "Pleasanton",
        "coordinate" => %{
          "geometry" => %{
            "coordinates" => [-121.93176070000001, 37.6936816],
            "type" => "Point"
          },
          "properties" => %{"name" => "Point"},
          "type" => "Feature"
        },
        "data_type" => "atm_locations_entity",
        "district" => "Dougherty",
        "id" => "85b5f364-9851-3ebb-83f7-f80af0302225",
        "latency" => 46,
        "published_at" => "2019-10-22T20:50:23.067Z",
        "published_by" => "85b5f364-9851-3ebb-83f7-f80af0302225",
        "publishing_template_type" => "516bccbe-f4ff-11e9-b63b-8c85900cbd75",
        "publishing_template_type_name" => "create_location_entities",
        "source" => "3171f670-f4fc-11e9-a42c-8c85900cbd75",
        "state" => "California",
        "zip_code" => 94588
      },
      event_definition: @event_definition,
      retry_count: 0
    }
  ]

  defp initialize_event_data() do
    # create event definition
    %EventDefinition{}
    |> EventDefinition.changeset(@event_definition)
    |> Repo.insert()

    # create event definition details
    Enum.each(@event_definition.fields, fn {key, value} ->
      %EventDefinitionDetail{}
      |> EventDefinitionDetail.changeset(%{
        event_definition_id: @event_definition.id,
        field_name: key,
        field_type: value
      })
      |> Repo.insert()
    end)

    # get system tag by name
    tag = Repo.get_by(SystemTag, name: "default")

    # create notification setting
    %NotificationSetting{}
    |> NotificationSetting.changeset(%{
      title: "city",
      active: true,
      event_definition_id: @event_definition.id,
      tag_id: tag.id
    })
    |> Repo.insert()
  end

  describe "Event Pipeline" do
    test "Pass 3 create event messages through the Event Pipeline" do
      initialize_event_data()

      ref = Broadway.test_messages(:BroadwayEventPipeline, @event_messages)

      # Assert that the messages have been consumed
      assert_receive {:ack, ^ref, successful, failed}, 1000
      assert length(successful) == 3
      assert length(failed) == 0

      # Assert database results
      assert Enum.count(Repo.all(Notification)) == 3
      assert Enum.count(Repo.all(Event)) == 3
      assert Enum.count(Repo.all(EventDetail)) == 45
    end
  end
end
