port module TopBar exposing (Model, Msg(..), init, update, urlUpdate, view, subscriptions, fetchUser)

import Dict
import Erl
import Html exposing (Html)
import Html.Attributes exposing (class, classList, href, id, disabled, attribute, style)
import Html.Events exposing (onClick)
import Http
import List
import Navigation exposing (Location)
import String
import Task
import Time

import Concourse
import Concourse.Pipeline
import Concourse.User
import Routes
import StrictEvents exposing (onLeftClickOrShiftLeftClick)

port toggleSidebar : () -> Cmd msg
port groupsChanged : List String -> Cmd msg
port selectGroups : (List String -> msg) -> Sub msg
port navigateTo : String -> Cmd msg
port setViewingPipeline : (Bool -> msg) -> Sub msg

type alias Model =
  { pipelineIdentifier : Maybe Concourse.PipelineIdentifier
  , viewingPipeline : Bool
  , ports : Ports
  , location : Routes.ConcourseRoute
  , groupsState : GroupsState
  , selectedGroups : List String
  , pipeline : Maybe Concourse.Pipeline
  , userState : UserState
  , userMenuVisible : Bool
  }

type UserState
  = UserStateLoggedIn Concourse.User
  | UserStateLoggedOut
  | UserStateUnknown

type GroupsState
  = GroupsStateSelected (List String)
  | GroupsStateNotLoaded

type alias Ports =
  { toggleSidebar : () -> Cmd Msg
  , setGroups : List String -> Cmd Msg
  , selectGroups : (List String -> Msg) -> Sub Msg
  , navigateTo : String -> Cmd Msg
  , setViewingPipeline : (Bool -> Msg) -> Sub Msg
  }

type Msg
  = Noop
  | PipelineFetched (Result Http.Error Concourse.Pipeline)
  | UserFetched (Result Concourse.User.Error Concourse.User)
  | FetchPipeline Concourse.PipelineIdentifier
  | ToggleSidebar
  | ToggleGroup Concourse.PipelineGroup
  | SetGroups (List String)
  | SelectGroups (List String)
  | LogOut
  | NavTo String
  | LoggedOut (Result Concourse.User.Error ())
  | ToggleUserMenu
  | SetViewingPipeline Bool

init : Routes.ConcourseRoute -> (Model, Cmd Msg)
init initialLocation =
  ( { pipelineIdentifier = Nothing
    , viewingPipeline = False
    , ports =
        { toggleSidebar = toggleSidebar
        , setGroups = groupsChanged
        , selectGroups = selectGroups
        , navigateTo = navigateTo
        , setViewingPipeline = setViewingPipeline
        }
    , groupsState = GroupsStateNotLoaded
    , location = initialLocation
    , selectedGroups = []
    , pipeline = Nothing
    , userState = UserStateUnknown
    , userMenuVisible = False
    }
  , fetchUser
  )

update : Msg -> Model -> (Model, Cmd Msg)
update msg model =
  case msg of
    Noop ->
      (model, Cmd.none)

    FetchPipeline pid ->
      (model, fetchPipeline pid)

    UserFetched (Ok user) ->
      ( { model | userState = UserStateLoggedIn user }
      , Cmd.none
      )

    UserFetched (Err _) ->
      ( { model | userState = UserStateLoggedOut }
      , Cmd.none
      )

    PipelineFetched (Ok pipeline) ->
      let
        firstGroup =
          List.head pipeline.groups
        model =
          { model | pipeline = Just pipeline }
      in
        case firstGroup of
          Nothing ->
            (model, Cmd.none)

          Just group ->
            case model.groupsState of
              GroupsStateNotLoaded ->
                (model, Cmd.none)

              GroupsStateSelected groups ->
                if List.length groups > 0 then
                  setGroups groups model
                else
                  setSelectedGroups [group.name] model

    PipelineFetched (Err err) ->
      Debug.log
        ("failed to load pipeline: " ++ toString err)
        (model, Cmd.none)

    ToggleSidebar ->
      Debug.log "sidebar-toggle-incorrectly-handled: " (model, Cmd.none)

    ToggleGroup group ->
      let
        newGroups =
          toggleGroup group model.selectedGroups model.pipeline
      in
        setGroups newGroups model

    SetGroups groups ->
      setGroups groups model

    SelectGroups groups ->
      setSelectedGroups groups model

    LogOut ->
      (model, logOut)

    LoggedOut (Ok _) ->
      ({ model | userState = UserStateLoggedOut }, Navigation.newUrl "/")

    NavTo url ->
      (model, Navigation.newUrl url)

    LoggedOut (Err msg) ->
      always (model, Cmd.none) <|
        Debug.log "failed to log out" msg

    ToggleUserMenu ->
      ({ model | userMenuVisible = not model.userMenuVisible }, Cmd.none)

    SetViewingPipeline vp ->
      ({ model | viewingPipeline = vp }, Cmd.none)

subscriptions : Model -> Sub Msg
subscriptions model =
  Sub.batch
    [ model.ports.setViewingPipeline SetViewingPipeline
    , model.ports.selectGroups SelectGroups
    , case model.pipelineIdentifier of
        Nothing ->
          Sub.none

        Just pid ->
          Time.every (5 * Time.second) (always (FetchPipeline pid))
    ]

setGroups : List String -> Model -> (Model, Cmd Msg)
setGroups newGroups model =
  case model.pipeline of
    Just pipeline ->
      let
        newUrl =
          locationToHistory pipeline <|
            setGroupsInLocation model.location newGroups
      in
        ( { model
          | selectedGroups = newGroups
          , groupsState = GroupsStateSelected newGroups
          }
        , if model.viewingPipeline then
            Cmd.batch
              [ Navigation.modifyUrl newUrl
              , model.ports.setGroups newGroups
              ]
          else
            model.ports.navigateTo newUrl
        )

    Nothing ->
      (model, Cmd.none)

setSelectedGroups : List String -> Model -> (Model, Cmd Msg)
setSelectedGroups groups model =
  ( { model | selectedGroups = groups }
  , if model.viewingPipeline then
      model.ports.setGroups groups
    else
      Cmd.none
  )

urlUpdate : Routes.ConcourseRoute -> Model -> (Model, Cmd Msg)
urlUpdate route model =
  ( { model
    | selectedGroups =
        case Dict.get "groups" route.parsed.query of
          Nothing -> [] -- TODO handle multiple
          Just group -> [ group ]
    , location = route
    }
  , Cmd.none
  )

setGroupsInLocation : Routes.ConcourseRoute -> List String -> Routes.ConcourseRoute
setGroupsInLocation loc groups =
  let
    updatedUrl =
      case List.head groups of
        Nothing ->
          Erl.removeQuery "groups" loc.parsed
        Just group ->
          Erl.addQuery "groups" group loc.parsed -- TODO handle multiple
  in
    { loc
    | parsed = updatedUrl
    }

locationToHistory : Concourse.Pipeline -> Routes.ConcourseRoute -> String
locationToHistory {url} {parsed} =
  String.join "" [url, Erl.queryToString parsed, parsed.hash]

toggleGroup : Concourse.PipelineGroup -> List String -> Maybe Concourse.Pipeline -> List String
toggleGroup group names mpipeline =
  let
    toggled =
      if List.member group.name names then
        List.filter ((/=) group.name) names
      else
        group.name :: names
  in
    defaultToFirstGroup toggled mpipeline

defaultToFirstGroup : List String -> Maybe Concourse.Pipeline -> List String
defaultToFirstGroup groups mpipeline =
  if List.isEmpty groups then
    case mpipeline of
      Just {groups} ->
        List.take 1 (List.map .name groups)

      Nothing ->
        []
  else
    groups

view : Model -> Html Msg
view model =
  Html.nav
    [ classList
        [ ("top-bar", True)
        , ("test", True)
        , ("paused", isPaused model.pipeline)
        ]
    ]
    [ let
        groupList =
          case model.pipeline of
            Nothing ->
              []
            Just pipeline ->
              List.map
                (viewGroup model.selectedGroups pipeline.url)
                  pipeline.groups
      in
        Html.ul [class "groups"] <|
          [ Html.li [class "main"]
              [ Html.span
                  [ class "sidebar-toggle test btn-hamburger"
                  , onClick ToggleSidebar
                  , Html.Attributes.attribute "aria-label" "Toggle List of Pipelines"
                  ]
                  [ Html.i [class "fa fa-bars"] []
                  ]
              ]
           , Html.li [class "main"]
              [ Html.a
                  [ StrictEvents.onLeftClick <| NavTo "/"
                  , Html.Attributes.href <|
                      Maybe.withDefault "/" (Maybe.map .url model.pipeline)
                  ]
                  [ Html.i [class "fa fa-home"] []
                  ]
              ]
          ] ++ groupList
    , Html.ul [class "nav-right"]
        [ Html.li [class "nav-item"]
            [ viewUserState model.userState model.userMenuVisible
            ]
        ]
    ]

isPaused : Maybe Concourse.Pipeline -> Bool
isPaused =
  Maybe.withDefault False << Maybe.map .paused

viewUserState : UserState -> Bool -> Html Msg
viewUserState userState userMenuVisible =
  case userState of
    UserStateUnknown ->
      Html.text ""

    UserStateLoggedOut ->
      Html.div [class "user-info"]
        [ Html.a
            [ StrictEvents.onLeftClick <| NavTo "/login"
            , href "/login"
            , Html.Attributes.attribute "aria-label" "Log In"
            , class "login-button"
            ]
            [ Html.text "login"
            ]
        ]

    UserStateLoggedIn {team} ->
      Html.div [class "user-info"]
        [ Html.div [class "user-id", onClick ToggleUserMenu]
            [ Html.i [class "fa fa-user"] []
            , Html.text " "
            , Html.text team.name
            , Html.text " "
            , Html.i [class "fa fa-caret-down"] []
            ]
        , Html.div [classList [("user-menu", True), ("hidden", not userMenuVisible)]]
            [ Html.a
                [ Html.Attributes.attribute "aria-label" "Log Out"
                , onClick LogOut
                ]
                [ Html.text "logout"
                ]
            ]
        ]

viewGroup : List String -> String -> Concourse.PipelineGroup -> Html Msg
viewGroup selectedGroups url grp =
  Html.li
    [ if List.member grp.name selectedGroups
        then class "main active"
        else class "main"
    ]
    [ Html.a
        [ Html.Attributes.href <| url ++ "?groups=" ++ grp.name
        , onLeftClickOrShiftLeftClick (SetGroups [grp.name]) (ToggleGroup grp)
        ]
        [ Html.text grp.name]
    ]

fetchPipeline : Concourse.PipelineIdentifier -> Cmd Msg
fetchPipeline pipelineIdentifier =
  Cmd.map PipelineFetched <|
    Task.perform Err Ok (Concourse.Pipeline.fetchPipeline pipelineIdentifier)

fetchUser : Cmd Msg
fetchUser =
  Cmd.map UserFetched <|
    Task.perform Err Ok Concourse.User.fetchUser

logOut : Cmd Msg
logOut =
  Cmd.map LoggedOut <|
    Task.perform Err Ok Concourse.User.logOut
