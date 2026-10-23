import copy

import pytest
from pydantic import ValidationError

from launchkit.blueprint import starter_blueprint
from launchkit.guide_models import DesignBrief, default_question, review_readiness, validate_guide_publication


def brief():
    return {
        "facts": {"outcome": {"summary": "Notice relevant project changes and help decide what needs attention.",
            "status": "confirmed", "source_message_ids": ["request"]}},
        "requirements": [{"id": "changes", "outcome": "Bring relevant project changes to the right agent.",
            "disposition": "open", "extension": "Resolve source capabilities and configure native delivery.",
            "source_message_ids": ["request"]}],
        "incoming_data": [{"id": "project_changes", "requirement_id": "changes", "name": "Project updates",
            "scope": "The team's selected project sources", "purpose": "Notice decisions that need follow-up.",
            "audience": "The project team", "history": "new_only", "routes": [
                {"recipient": "lead:main", "delivery": "direct"},
                {"recipient": "review:main", "delivery": "triage"}], "source_message_ids": ["request"]}],
    }


def publish(value, plan=None):
    return validate_guide_publication({"message": "Choose which changes matter most.", "brief": value,
        "question": default_question("connections"), "design_changed": True},
        {"brief": {}, "evidence_messages": [{"id": "request", "role": "user", "text": "Help with project changes"}],
         "current_user_message": {"id": "request", "role": "user"}},
        plan or starter_blueprint().model_dump(mode="json"))


def test_old_brief_serialization_and_fingerprint_inputs_stay_unchanged():
    old = {"facts": {}, "requirements": []}
    assert DesignBrief.model_validate(old).model_dump(mode="json") == old
    assert DesignBrief.model_validate({**old, "incoming_data": []}).model_dump(mode="json") == old


def test_guided_publication_retains_mixed_routes_as_intent_and_open_implementation():
    value = brief()
    before = copy.deepcopy(value)
    result = publish(value)
    assert result["brief"]["incoming_data"][0]["routes"] == value["incoming_data"][0]["routes"]
    assert result["brief"]["requirements"][0]["disposition"] == "open"
    assert any("Bring relevant" in item for item in review_readiness(result["brief"])["implementation_blockers"])
    assert value == before


@pytest.mark.parametrize("class_id", ["review", "q", "qa"])
def test_class_route_can_start_empty_and_survives_reopen_without_placeholder_members(tmp_path, class_id):
    from launchkit.guide_store import GuideStore

    plan = starter_blueprint().model_dump(mode="json")
    recipient_class = next(item for item in plan["agent_classes"] if item["id"] == "review")
    recipient_class.update(id=class_id, instances=0)
    plan["flows"] = []
    value = brief()
    source = value["incoming_data"][0]
    source["routes"] = [{"recipient": class_id, "recipient_kind": "class", "delivery": "direct"}]
    before = copy.deepcopy(plan)
    result = publish(value, plan)
    saved = GuideStore(tmp_path).create({"brief": result["brief"], "proposal": plan})
    reopened = GuideStore(tmp_path).get(saved["id"])
    assert reopened["brief"]["incoming_data"][0]["routes"] == source["routes"]
    assert next(item for item in reopened["proposal"]["agent_classes"] if item["id"] == class_id)["instances"] == 0
    assert reopened["brief"]["requirements"][0]["disposition"] == "open"
    assert plan == before


@pytest.mark.parametrize("recipient", ["Q", "qa team", "a" * 41, "lead:main"])
def test_class_recipients_follow_blueprint_id_rules(recipient):
    from launchkit.guide_models import IncomingDataRoute

    with pytest.raises(ValueError, match="class ID"):
        IncomingDataRoute(recipient=recipient, recipient_kind="class", delivery="direct")


@pytest.mark.parametrize("recipient", ["q", "qa"])
def test_short_class_ids_do_not_relax_identity_recipient_validation(recipient):
    from launchkit.guide_models import IncomingDataRoute

    with pytest.raises(ValueError):
        IncomingDataRoute(recipient=recipient, delivery="direct")


@pytest.mark.parametrize("delivery", ["triage", "conversation"])
def test_class_cannot_be_an_ambiguous_triage_or_conversation_owner(delivery):
    value = brief()
    value["incoming_data"][0]["routes"] = [{"recipient": "review", "recipient_kind": "class", "delivery": delivery}]
    with pytest.raises(ValueError, match="one identity"):
        publish(value)


@pytest.mark.parametrize("class_first", [True, False])
def test_class_and_known_member_cannot_receive_overlapping_routes(class_first):
    value = brief()
    routes = [{"recipient": "lead", "recipient_kind": "class", "delivery": "direct"},
              {"recipient": "lead:main", "delivery": "conversation"}]
    value["incoming_data"][0]["routes"] = routes if class_first else routes[::-1]
    with pytest.raises(ValueError, match="class and identity routes must not overlap"):
        publish(value)


def test_removed_class_cannot_remain_a_recipient():
    value = brief()
    value["incoming_data"][0]["routes"] = [{"recipient": "removed", "recipient_kind": "class", "delivery": "direct"}]
    with pytest.raises(ValueError, match="existing class"):
        publish(value)


@pytest.mark.parametrize("change,error", [
    (lambda s: s["routes"].append({"recipient": "lead:main", "delivery": "conversation"}), "one incoming route"),
    (lambda s: s["routes"].append({"recipient": "lead:main", "delivery": "triage"}), "one incoming route"),
    (lambda s: s.update(history="bounded_history"), "history window"),
    (lambda s: s.update(history_window="Last month"), "bounded-history"),
    (lambda s: s.update(requirement_id="unknown"), "existing requested feature"),
    (lambda s: s.update(source_message_ids=["assistant_message"]), "non-user evidence"),
    (lambda s: s.update(routes=[{"recipient": "missing:main", "delivery": "direct"}]), "existing identity"),
])
def test_invalid_or_unsupported_desired_routes_do_not_publish(change, error):
    value = brief()
    change(value["incoming_data"][0])
    with pytest.raises((ValueError, ValidationError), match=error):
        publish(value)


def test_unknown_recipients_and_explicit_history_remain_valid_design_work():
    value = brief()
    source = value["incoming_data"][0]
    source.update(routes=[], history="bounded_history", history_window="The current project's last two weeks")
    result = publish(value)
    assert result["brief"]["incoming_data"][0]["routes"] == []
    assert result["brief"]["incoming_data"][0]["history_window"] == source["history_window"]


def test_restructured_team_cannot_leave_an_incoming_route_to_a_removed_member():
    plan = starter_blueprint().model_dump(mode="json")
    plan["agent_classes"] = plan["agent_classes"][:1]
    plan["flows"] = []
    with pytest.raises(ValueError, match="existing identity"):
        publish(brief(), plan)


def test_unchanged_team_can_continue_the_interview_with_saved_incoming_routes():
    value = brief()
    result = validate_guide_publication({"message": "Choose a history window.", "brief": value,
        "question": default_question("connections"), "design_changed": False},
        {"brief": value, "evidence_messages": [{"id": "request", "role": "user"}]},
        None, current_blueprint=starter_blueprint().model_dump(mode="json"))
    assert result["brief"]["incoming_data"][0]["routes"] == value["incoming_data"][0]["routes"]


def test_incoming_choices_survive_saved_setup_reopen_and_remain_in_agent_context(tmp_path):
    from launchkit.guide import compact_guidance_context
    from launchkit.guide_store import GuideStore
    from launchkit.guided_edits import guided_document

    value = publish(brief())["brief"]
    plan = starter_blueprint().model_dump(mode="json")
    saved = GuideStore(tmp_path).create({"brief": value, "proposal": plan})
    reopened = GuideStore(tmp_path).get(saved["id"])
    assert reopened["brief"] == value
    assert compact_guidance_context({"brief": reopened["brief"]})["brief"] == value
    assert guided_document(plan, reopened["brief"])["brief"] == value


@pytest.mark.parametrize("recipient_kind,recipient", [("identity", "lead:main"), ("class", "lead")])
@pytest.mark.asyncio
async def test_starter_scaffolding_cannot_supply_recipients_at_publication_tool(tmp_path, recipient_kind, recipient):
    from launchkit.architect import ArchitectService

    author = ArchitectService(tmp_path)
    author.context = starter_blueprint().model_dump(mode="json")
    author.guide_context = {"saved_plan_available": False, "brief": {},
        "evidence_messages": [{"id": "request", "role": "user"}]}
    value = brief()
    value["facts"]["outcome"]["status"] = "proposed"
    value["incoming_data"][0]["routes"] = [
        {"recipient": recipient, "recipient_kind": recipient_kind, "delivery": "direct"}]
    result = await author.publish_guide_step({"message": "Which changes should matter?", "brief": value,
        "question": default_question("purpose"), "design_changed": False})
    assert result["valid"] is False
    assert "Propose the team" in result["issue"]
    assert author.guide_result is None and author.proposal is None


@pytest.mark.parametrize("plan_source", ["new_proposal", "saved_proposal", "manual_proposal"])
def test_real_team_may_keep_starter_identity_names(plan_source):
    plan = starter_blueprint().model_dump(mode="json")
    value = brief()
    proposed = plan_source != "saved_proposal"
    result = validate_guide_publication({"message": "Which project sources should the team use?", "brief": value,
        "question": default_question("connections"), "design_changed": proposed},
        {"brief": {}, "saved_plan_available": not proposed,
         "action": "manual" if plan_source == "manual_proposal" else "chat",
         "manual_plan_submitted": plan_source == "manual_proposal",
         "evidence_messages": [{"id": "request", "role": "user"}]},
        plan if proposed else None, current_blueprint=plan)
    assert result["brief"] == DesignBrief.model_validate(value).model_dump(mode="json")


@pytest.mark.asyncio
@pytest.mark.parametrize("routes", [[], [{"recipient": "lead:main", "delivery": "direct"}],
    [{"recipient": "lead", "recipient_kind": "class", "delivery": "direct"}]])
async def test_first_guide_turn_saves_only_unassigned_source_intent(tmp_path, monkeypatch, routes):
    from launchkit.guide import GuideService
    from launchkit.guide_store import GuideStore
    from launchkit.server import GuideResponse

    monkeypatch.setattr("launchkit.guide.planner_capability", lambda: {"available": True})

    class Author:
        async def message(self, prompt, blueprint, *, guide_context):
            assert guide_context["saved_plan_available"] is False
            assert guide_context["manual_plan_submitted"] is False
            assert blueprint["agent_classes"][0]["id"] == "lead"
            value = brief()
            value["facts"]["outcome"]["status"] = "proposed"
            value["incoming_data"][0]["routes"] = routes
            for item in [*value["facts"].values(), *value["requirements"], *value["incoming_data"]]:
                item["source_message_ids"] = [guide_context["current_user_message"]["id"]]
            return {"guide": {"message": "Which changes should matter?", "brief": value,
                "question": default_question("purpose"), "design_changed": False}, "proposal": None}

    store = GuideStore(tmp_path)
    service = GuideService(store, Author())
    original = await service.create()
    before = copy.deepcopy(store.get(original["id"]))
    request = GuideResponse(version=original["version"], action="chat", text="Help me notice project changes.")
    if routes:
        with pytest.raises(ValueError, match="Propose the team"):
            await service.respond(original["id"], request)
        assert store.get(original["id"]) == before
    else:
        result = await service.respond(original["id"], request)
        reopened = GuideStore(tmp_path).get(original["id"])
        assert result["version"] == original["version"] + 1
        assert reopened["brief"]["incoming_data"][0]["routes"] == []
        assert reopened["proposal"] is None and reopened["blueprint"] is None
