from __future__ import annotations

import json
from typing import Any, Awaitable, Callable

from pipecat_flows import FlowsFunctionSchema, NodeConfig

from src.config import (
    DTMF_SUBMIT_KEY,
    IDENTIFIER_MAX_ATTEMPTS,
    VERIFICATION_MAX_ATTEMPTS,
)
from src.packs.isp.contracts import (
    CHECK_OUTAGE_TOOL_ID,
    CREATE_TICKET_TOOL_ID,
    LOAD_SERVICE_CONTEXT_TOOL_ID,
    LOOKUP_CUSTOMER_TOOL_ID,
    SEND_SMS_TOOL_ID,
    VERIFY_CUSTOMER_TOOL_ID,
)
from src.pipecat_runtime.tools import PipecatToolRuntime


_ROLE_MESSAGE = (
    "იმუშავეთ მიმდინარე ეტაპის ამოცანის მიხედვით. "
    "პასუხი იყოს მოკლე და მაქსიმუმ ერთი კითხვა. "
    "გამოიყენეთ მხოლოდ მიმდინარე ეტაპზე ხელმისაწვდომი ფუნქციები. "
    "როცა ზარი დასრულებულია, გამოიძახეთ end_call."
)

_PROMPT_CONTEXT_FIELDS = (
    "caller_number",
    "subscriber_identifier",
    "lookup_status",
    "lookup_message",
    "verification_status",
    "verification_reason",
    "customer_id",
    "account_id",
    "company_name",
    "customer_name",
    "contact_phone",
    "callback_number",
    "service_id",
    "service_type",
    "service_packet",
    "account_status",
    "service_address",
    "verification_hint",
    "outage_status",
    "outage_summary",
    "outage_eta_text",
    "ticket_id",
    "balance_verified",
)
_PROMPT_CONTEXT_SCALAR_LIMIT = 8
_PROMPT_CONTEXT_STR_LIMIT = 160


def build_isp_router_node(tool_runtime: PipecatToolRuntime) -> NodeConfig:
    return _build_node(
        name="isp_router",
        tool_runtime=tool_runtime,
        task_messages=[
            _developer_message(
                "ამოცანა: მხოლოდ მიზეზის ამოცნობა და სწორი მიმართულებით გადამისამართება.\n\n"
                "მიმართულებები:\n"
                "- route_to_technical_support → ინტერნეტი, Wi-Fi, მოდემი, ONT, სიჩქარე, კავშირის პრობლემა\n"
                "- route_to_billing → ზუსტი ბალანსი ან დაცული საბილინგო დეტალი\n"
                "- route_to_human_handoff → ადამიანი სურს, გაღიზიანებულია, ბუნდოვანია, გაყიდვები/ინსტალაცია/სხვა საკითხი\n\n"
                "წესები:\n"
                "- თუ მართლა საჭიროა, დასვი მხოლოდ ერთი მოკლე დამაზუსტებელი კითხვა\n"
                "- თუ ისევ ბუნდოვანია → route_to_human_handoff"
            )
        ],
        functions=[
            _route_function(
                name="route_to_technical_support",
                description="Route the caller into the technical support branch.",
                next_node_factory=lambda: _build_technical_entry_node(tool_runtime),
                branch_name="technical_support",
            ),
            _route_function(
                name="route_to_billing",
                description="Route the caller into the billing and account branch.",
                next_node_factory=lambda: build_billing_node(tool_runtime),
                branch_name="billing",
            ),
            _route_function(
                name="route_to_human_handoff",
                description="Route the caller into human handoff for complaints, unclear intent, unsupported requests, or a requested escalation.",
                next_node_factory=lambda: build_human_handoff_node(tool_runtime),
                branch_name="human_handoff",
            ),
        ],
        respond_immediately=False,
    )


def build_technical_support_node(
    tool_runtime: PipecatToolRuntime,
    retry_count: int = 0,
) -> NodeConfig:
    return _build_node(
        name="isp_technical_support",
        tool_runtime=tool_runtime,
        task_messages=[
            _developer_message(
                "თქვენ ხართ ტექნიკური მხარდაჭერის იდენტიფიკაციის ეტაპზე.\n\n"
                "ეს ეტაპი გამოიყენება მხოლოდ სააბონენტო ან ანგარიშის იდენტიფიკატორის განსაზღვრული კლავიატურული შეგროვებისთვის.\n"
                f"{_technical_identifier_prompt(retry_count)}\n\n"
                "წესები:\n"
                "- სთხოვეთ მხოლოდ კლავიატურით შეყვანა.\n"
                f"- უთხარით აბონენტს, რომ ტელეფონის კლავიატურით შეიყვანოს სააბონენტო ან ანგარიშის ნომერი და { _submit_key_instruction() }\n"
                "- ნუ სთხოვთ, რომ მნიშვნელოვანი ციფრები ხმამაღლა თქვას.\n"
                "- თუ აბონენტი ითხოვს ზუსტ ბალანსს ან სხვა დაცულ საბილინგო დეტალს, გამოიძახეთ route_to_billing.\n"
                "- თუ აბონენტს ადამიანი უნდა, ჩივის ან გაგრძელებაზე უარს ამბობს, გამოიძახეთ route_to_human_handoff."
            )
        ],
        functions=[
            _route_function(
                name="route_to_billing",
                description="Move the caller to billing if the issue is account or payment related.",
                next_node_factory=lambda: build_billing_node(tool_runtime),
                branch_name="billing",
            ),
            _route_function(
                name="route_to_human_handoff",
                description="Escalate the caller to human handoff at any point.",
                next_node_factory=lambda: build_human_handoff_node(tool_runtime),
                branch_name="human_handoff",
            ),
        ],
        pre_actions=[
            _tts_say_action(_technical_identifier_tts_text(retry_count)),
            _start_dtmf_collection_action(
                kind="subscriber_identifier",
                on_complete=_technical_identifier_complete_handler(
                    tool_runtime,
                    retry_count=retry_count,
                ),
            )
        ],
        respond_immediately=False,
    )


def build_technical_diagnosis_node(tool_runtime: PipecatToolRuntime) -> NodeConfig:
    return _build_node(
        name="isp_technical_diagnose",
        tool_runtime=tool_runtime,
        task_messages=[
            _developer_message(
                "თქვენ ხართ ტექნიკური დიაგნოსტიკის ეტაპზე.\n\n"
                "ამ ეტაპამდე უკვე შესრულდა განსაზღვრული წინაპირობები:\n"
                "- აბონენტის მოძიება დასრულებულია\n"
                "- სერვისის კონტექსტი ჩატვირთულია\n\n"
                "წესები:\n"
                "- დიაგნოსტიკა დაიწყეთ ქვემოთ მოცემული ცნობილი კონტექსტით.\n"
                "- სააბონენტო იდენტიფიკატორი ხელახლა არ მოითხოვოთ, თუ აბონენტი თავად არ ცვლის ანგარიშს.\n"
                "- ტექნიკური დიაგნოსტიკისთვის ვერიფიკაცია არ მოითხოვოთ.\n"
                "- თუ აბონენტი ითხოვს ზუსტ ბალანსს ან სხვა დაცულ საბილინგო დეტალს, გამოიძახეთ route_to_billing.\n"
                "- თუ ცნობილი კონტექსტი აშკარად მიუთითებს პროვაიდერის მხარის პრობლემაზე ან სხვა შემთხვევაზე, რომელსაც შემდგომი რეაგირება სჭირდება, მოკლედ აუხსენით და გამოიძახეთ route_to_human_handoff.\n"
                "- თუ საკითხი ზოგად ბილინგურ ან ანგარიშის მხარდაჭერას ეხება, რომელსაც აქ ვერ ვამუშავებთ, გამოიძახეთ route_to_human_handoff.\n"
                "- თუ მიზეზი ჯერ ისევ ბუნდოვანია, შეგიძლიათ გამოიძახოთ load_service_context ან check_outage, შემდეგ კი ან მოკლედ გააგრძელოთ, ან გამოიძახოთ route_to_basic_troubleshooting.\n"
                "- დიაგნოსტიკა იყოს მოკლე და პრაქტიკული."
            ),
            _known_context_message(tool_runtime),
        ],
        functions=[
            _tool_function(tool_runtime, LOAD_SERVICE_CONTEXT_TOOL_ID),
            _tool_function(tool_runtime, CHECK_OUTAGE_TOOL_ID),
            _route_function(
                name="route_to_basic_troubleshooting",
                description="Move the call into short first-line troubleshooting steps.",
                next_node_factory=lambda: build_basic_troubleshooting_node(tool_runtime),
                branch_name="technical_support",
            ),
            _route_function(
                name="route_to_billing",
                description="Move the call into billing if the issue is account or payment related.",
                next_node_factory=lambda: build_billing_node(tool_runtime),
                branch_name="billing",
            ),
            _route_function(
                name="route_to_human_handoff",
                description="Escalate the caller into human handoff with the current context.",
                next_node_factory=lambda: build_human_handoff_node(tool_runtime),
                branch_name="human_handoff",
            ),
        ],
    )


def build_basic_troubleshooting_node(tool_runtime: PipecatToolRuntime) -> NodeConfig:
    return _build_node(
        name="isp_technical_basic_troubleshooting",
        tool_runtime=tool_runtime,
        task_messages=[
            _developer_message(
                "თქვენ ხართ მოკლე პირველადი ტექნიკური დახმარების ეტაპზე.\n\n"
                "წესები:\n"
                "- გამოიყენეთ ქვემოთ მოცემული ცნობილი კონტექსტი.\n"
                "- ერთ ჯერზე მხოლოდ ერთი მოკლე ნაბიჯი მიეცით.\n"
                "- დიალოგი ზედმეტად არ გაწელოთ.\n"
                "- თუ პრობლემა მოგვარდა და დამატებითი ქმედება აღარ არის საჭირო, მოკლედ შეაჯამეთ და გამოიძახეთ end_call.\n"
                "- თუ აბონენტი ითხოვს ზუსტ ბალანსს ან სხვა დაცულ საბილინგო დეტალს, გამოიძახეთ route_to_billing.\n"
                "- თუ საკითხი ზოგად ბილინგურ მხარდაჭერას ეხება და არა ზუსტ ბალანსს, გამოიძახეთ route_to_human_handoff.\n"
                "- თუ პრობლემა სწრაფად არ გვარდება, აბონენტი გაღიზიანებულია ან საჭიროა ადამიანის შემდგომი ჩართვა, გამოიძახეთ route_to_human_handoff."
            ),
            _known_context_message(tool_runtime),
        ],
        functions=[
            _route_function(
                name="route_to_billing",
                description="Move the call to billing if the issue turns out to be account or payment related.",
                next_node_factory=lambda: build_billing_node(tool_runtime),
                branch_name="billing",
            ),
            _route_function(
                name="route_to_human_handoff",
                description="Escalate the unresolved or upset caller with the gathered context.",
                next_node_factory=lambda: build_human_handoff_node(tool_runtime),
                branch_name="human_handoff",
            ),
        ],
    )


def build_billing_node(tool_runtime: PipecatToolRuntime) -> NodeConfig:
    return _build_node(
        name="isp_billing",
        tool_runtime=tool_runtime,
        task_messages=[
            _developer_message(
                "თქვენ ხართ ზუსტი ბალანსის და დაცული საბილინგო დეტალების ეტაპზე.\n\n"
                "წესები:\n"
                "- როცა არსებობს, გამოიყენეთ ქვემოთ მოცემული ცნობილი კონტექსტი.\n"
                "- ეს ეტაპი გამოიყენეთ მხოლოდ მაშინ, როცა აბონენტი ითხოვს ზუსტ ბალანსს ან სხვა დაცულ საბილინგო დეტალს.\n"
                "- ვერიფიკაცია საჭიროა მხოლოდ ზუსტი ბალანსის ან სხვა დაცული ანგარიშის დეტალის გასამჟღავნებლად.\n"
                "- ნუ სთხოვთ აბონენტს, რომ მნიშვნელოვანი ციფრები ხმამაღლა თქვას.\n"
                f"- თუ ზუსტი ბალანსი ან დაცული ანგარიშის დეტალებია საჭირო და აბონენტის კონტექსტი უცნობია, გამოიძახეთ route_to_billing_identifier_collection, რათა იდენტიფიკატორი კლავიატურით შეიყვანოს და { _submit_key_instruction() }\n"
                "- თუ ზუსტი ბალანსი ან დაცული ანგარიშის დეტალებია საჭირო და აბონენტის კონტექსტი უკვე ცნობილია, გამოიძახეთ route_to_balance_verification.\n"
                "- ტექნიკურ საკითხებზე გამოიძახეთ route_to_technical_support.\n"
                "- თუ აბონენტს სხვა საბილინგო დახმარება სჭირდება, რომელიც ზუსტ ბალანსს არ ეხება, გამოიძახეთ route_to_human_handoff.\n"
                "- თუ აბონენტს ადამიანი უნდა, გაღიზიანებულია ან შემთხვევა აქ ვერ სრულდება, გამოიძახეთ route_to_human_handoff."
            ),
            _known_context_message(tool_runtime),
        ],
        functions=[
            _route_function(
                name="route_to_billing_identifier_collection",
                description="Collect the subscriber or account identifier by keypad before protected billing disclosure.",
                next_node_factory=lambda: build_billing_identifier_collection_node(tool_runtime),
                branch_name="billing",
            ),
            _balance_verification_route_function(tool_runtime),
            _route_function(
                name="route_to_technical_support",
                description="Move the caller into technical support if the issue is service-related.",
                next_node_factory=lambda: _build_technical_entry_node(tool_runtime),
                branch_name="technical_support",
            ),
            _route_function(
                name="route_to_human_handoff",
                description="Escalate the caller to human handoff with the current context.",
                next_node_factory=lambda: build_human_handoff_node(tool_runtime),
                branch_name="human_handoff",
            ),
        ],
    )


def build_billing_identifier_collection_node(
    tool_runtime: PipecatToolRuntime,
    retry_count: int = 0,
) -> NodeConfig:
    return _build_node(
        name="isp_billing_identifier_collection",
        tool_runtime=tool_runtime,
        task_messages=[
            _developer_message(
                "თქვენ ბილინგისათვის კლავიატურით აგროვებთ სააბონენტო იდენტიფიკატორს.\n\n"
                f"{_billing_identifier_prompt(retry_count)}\n\n"
                "წესები:\n"
                f"- სთხოვეთ აბონენტს, კლავიატურით შეიყვანოს სააბონენტო ან ანგარიშის ნომერი და { _submit_key_instruction() }\n"
                "- ნუ სთხოვთ, რომ ნომერი ხმამაღლა თქვას.\n"
                "- თუ აბონენტს ამის ნაცვლად ადამიანი უნდა, გამოიძახეთ route_to_human_handoff."
            )
        ],
        functions=[
            _route_function(
                name="route_to_human_handoff",
                description="Escalate the caller instead of continuing billing verification.",
                next_node_factory=lambda: build_human_handoff_node(tool_runtime),
                branch_name="human_handoff",
            )
        ],
        pre_actions=[
            _tts_say_action(_billing_identifier_tts_text(retry_count)),
            _start_dtmf_collection_action(
                kind="billing_identifier",
                on_complete=_billing_identifier_complete_handler(
                    tool_runtime,
                    retry_count=retry_count,
                ),
            )
        ],
        respond_immediately=False,
    )


def build_billing_verification_node(
    tool_runtime: PipecatToolRuntime,
    retry_count: int = 0,
) -> NodeConfig:
    verification_hint = tool_runtime.conversation_context().get("verification_hint")
    hint_text = (
        f"Known verification hint: {verification_hint}. "
        if verification_hint
        else ""
    )
    return _build_node(
        name="isp_billing_verification",
        tool_runtime=tool_runtime,
        task_messages=[
            _developer_message(
                "თქვენ ზუსტი ბალანსის გამჟღავნებამდე ბილინგის ვერიფიკაციას კლავიატურით აგროვებთ.\n\n"
                f"{_billing_verification_prompt(retry_count)}\n\n"
                "წესები:\n"
                f"- მოითხოვეთ მხოლოდ კლავიატურით შეყვანა და უთხარით აბონენტს, რომ { _submit_key_instruction() }\n"
                "- ნუ სთხოვთ, რომ მნიშვნელოვანი ციფრები ხმამაღლა თქვას.\n"
                f"- {hint_text}ვერიფიკაცია გამოიყენეთ მხოლოდ ზუსტი ბალანსის ან სხვა დაცული ანგარიშის ნომრის გასამჟღავნებლად.\n"
                "- თუ აბონენტი ვერიფიკაციაზე უარს ამბობს ან ადამიანს ითხოვს, გამოიძახეთ route_to_human_handoff."
            ),
            _known_context_message(tool_runtime),
        ],
        functions=[
            _route_function(
                name="route_to_human_handoff",
                description="Escalate the caller instead of continuing billing verification.",
                next_node_factory=lambda: build_human_handoff_node(tool_runtime),
                branch_name="human_handoff",
            )
        ],
        pre_actions=[
            _tts_say_action(_billing_verification_tts_text(retry_count)),
            _start_dtmf_collection_action(
                kind="billing_verification",
                on_complete=_billing_verification_complete_handler(
                    tool_runtime,
                    retry_count=retry_count,
                ),
            )
        ],
        respond_immediately=False,
    )


def build_billing_exact_balance_node(tool_runtime: PipecatToolRuntime) -> NodeConfig:
    return _build_node(
        name="isp_billing_exact_balance",
        tool_runtime=tool_runtime,
        task_messages=[
            _developer_message(
                "თქვენ ხართ ბილინგის დადასტურებული გამჟღავნების ეტაპზე.\n\n"
                "წესები:\n"
                "- ამ ბალანსის მოთხოვნაზე ვერიფიკაცია უკვე წარმატებით დასრულდა.\n"
                "- ზუსტი ბალანსის გაზიარებამდე გამოიძახეთ get_cached_balance.\n"
                "- ბალანსი მეხსიერებიდან არ მოიგონოთ, არ დაამრგვალოთ და არ გაიმეოროთ წყაროს გარეშე.\n"
                "- როცა ზუსტი ბალანსი მიაწოდეთ და დამატებითი მოთხოვნა აღარ დარჩა, გამოიძახეთ end_call.\n"
                "- თუ აბონენტის საკითხი ტექნიკურ მხარდაჭერაზე გადავა, გამოიძახეთ route_to_technical_support.\n"
                "- თუ საჭიროა ადამიანის შემდგომი რეაგირება, გამოიძახეთ route_to_human_handoff."
            ),
            _known_context_message(tool_runtime),
        ],
        functions=[
            _cached_balance_function(tool_runtime),
            _route_function(
                name="route_to_technical_support",
                description="Move the caller into technical support if the issue changes to service troubleshooting.",
                next_node_factory=lambda: _build_technical_entry_node(tool_runtime),
                branch_name="technical_support",
            ),
            _route_function(
                name="route_to_human_handoff",
                description="Escalate the caller to human handoff with the verified context.",
                next_node_factory=lambda: build_human_handoff_node(tool_runtime),
                branch_name="human_handoff",
            ),
        ],
    )


def build_human_handoff_node(tool_runtime: PipecatToolRuntime) -> NodeConfig:
    return _build_node(
        name="isp_human_handoff",
        tool_runtime=tool_runtime,
        task_messages=[
            _developer_message(
                "თქვენ ხართ ადამიანის შემდგომი ჩართვის ეტაპზე.\n\n"
                "წესები:\n"
                "- ეს ეტაპი ხელმისაწვდომია ნებისმიერი ადგილიდან, როცა აბონენტი ჩივის, მოთხოვნა ბუნდოვანია, საკითხი ვერ გვარდება ან პირდაპირ ადამიანს ითხოვს.\n"
                "- ამ ვერსიაში ცოცხალი გადართვა არ არის ხელმისაწვდომი; აუხსენით, რომ შეიქმნება ტიკეტი და საჭიროების შემთხვევაში დაიგეგმება უკუგამოძახება.\n"
                "- საუბარი იყოს მოკლე, მშვიდი და მკაფიო.\n"
                "- ხელმისაწვდომი ტიკეტის რეზიუმე შექმენით ქვემოთ მოცემული ცნობილი კონტექსტის საფუძველზე.\n"
                "- თუ lookup_status არის not_found, გამოიყენეთ create_ticket ticket_kind=subscriber_not_found.\n"
                "- თუ verification_status არის failed, გამოიყენეთ create_ticket ticket_kind=verification_failed.\n"
                "- თუ outage_status არის outage, გამოიყენეთ create_ticket ticket_kind=outage.\n"
                "- თუ შემთხვევა მოუგვარებელია და ტექნიკური ან ბილინგის ჯგუფის ჩართვა სჭირდება, ჩვეულებრივ გამოიყენეთ create_ticket ticket_kind=escalation.\n"
                "- თუ აბონენტს ძირითადად უკუგამოძახება უნდა, გამოიყენეთ create_ticket ticket_kind=callback_request.\n"
                "- თუ ცნობილი საკონტაქტო ნომერი არსებობს, ჰკითხეთ, უკუგამოძახება ამ ნომერზე სურს თუ არა.\n"
                "- თუ აბონენტს სხვა ნომერი უნდა, გამოიძახეთ collect_alternate_callback_number, რათა ნომერი კლავიატურით შეიყვანოს.\n"
                "- ნუ სთხოვთ აბონენტს, რომ მნიშვნელოვანი ციფრები ხმამაღლა თქვას.\n"
                "- თუ შემდგომი რეაგირებაა საჭირო, დასრულებამდე შექმენით ტიკეტი.\n"
                "- SMS გააგზავნეთ მხოლოდ მაშინ, როცა ეს რეალურად საჭიროა და ტიკეტი უკვე შექმნილია.\n"
                "- როცა ტიკეტი შექმნილია, უკუგამოძახების დეტალი შეთანხმებულია და სხვა პასუხს აღარ ელით, გამოიძახეთ end_call."
            ),
            _known_context_message(tool_runtime),
        ],
        functions=[
            _tool_function(tool_runtime, CREATE_TICKET_TOOL_ID),
            _tool_function(tool_runtime, SEND_SMS_TOOL_ID),
            _route_function(
                name="collect_alternate_callback_number",
                description="Collect an alternate callback number by keypad and return here.",
                next_node_factory=lambda: build_handoff_callback_collection_node(tool_runtime),
                branch_name="human_handoff",
            ),
        ],
    )


def build_handoff_callback_collection_node(tool_runtime: PipecatToolRuntime) -> NodeConfig:
    return _build_node(
        name="isp_handoff_callback_collection",
        tool_runtime=tool_runtime,
        task_messages=[
            _developer_message(
                "თქვენ ადამიანური შემდგომი ჩართვისთვის ალტერნატიულ უკუგამოძახების ნომერს აგროვებთ.\n\n"
                f"- სთხოვეთ აბონენტს, კლავიატურით შეიყვანოს უკუგამოძახების ნომერი და { _submit_key_instruction() }\n"
                "- ნუ სთხოვთ, რომ ნომერი ხმამაღლა თქვას.\n"
                "- შეგროვების შემდეგ მართვა ბრუნდება human handoff-ის ეტაპზე.\n"
                "- თუ აბონენტი გადაწყვეტს არსებული ნომრის დატოვებას, გამოიძახეთ route_to_human_handoff."
            ),
            _known_context_message(tool_runtime),
        ],
        functions=[
            _route_function(
                name="route_to_human_handoff",
                description="Return to the human handoff node and use the current contact number.",
                next_node_factory=lambda: build_human_handoff_node(tool_runtime),
                branch_name="human_handoff",
            )
        ],
        pre_actions=[
            _tts_say_action(_callback_number_tts_text()),
            _start_dtmf_collection_action(
                kind="callback_number",
                on_complete=_handoff_callback_complete_handler(tool_runtime),
            )
        ],
        respond_immediately=False,
    )


def _build_technical_entry_node(tool_runtime: PipecatToolRuntime) -> NodeConfig:
    if tool_runtime.has_customer_context():
        return build_technical_diagnosis_node(tool_runtime)
    return build_technical_support_node(tool_runtime)


def _build_node(
    *,
    name: str,
    tool_runtime: PipecatToolRuntime,
    task_messages: list[dict[str, str]],
    functions: list[FlowsFunctionSchema],
    pre_actions: list[dict[str, Any]] | None = None,
    post_actions: list[dict[str, Any]] | None = None,
    respond_immediately: bool = True,
) -> NodeConfig:
    node_functions = list(functions)
    if not any(function.name == "end_call" for function in node_functions):
        node_functions.append(_end_call_function(tool_runtime))
    node: NodeConfig = {
        "name": name,
        "role_message": _ROLE_MESSAGE,
        "task_messages": task_messages,
        "functions": node_functions,
        "pre_actions": [_clear_dtmf_collection_action(), *(pre_actions or [])],
        "respond_immediately": respond_immediately,
    }
    if post_actions:
        node["post_actions"] = post_actions
    return node


def _technical_identifier_prompt(retry_count: int) -> str:
    if retry_count <= 0:
        return "ახლა სთხოვეთ სააბონენტო ან ანგარიშის ნომერი."
    return "წინა იდენტიფიკატორით გამოსადეგი აბონენტის ჩანაწერი ვერ მოიძებნა. სთხოვეთ აბონენტს, კლავიატურით თავიდან სცადოს."


def _billing_identifier_prompt(retry_count: int) -> str:
    if retry_count <= 0:
        return "ახლა სთხოვეთ სააბონენტო ან ანგარიშის ნომერი, რათა დაცულ ბილინგის დეტალებზე გადასვლა გაგრძელდეს."
    return "წინა იდენტიფიკატორით გამოსადეგი აბონენტის ჩანაწერი ვერ მოიძებნა. სთხოვეთ კლავიატურით თავიდან შეყვანა."


def _billing_verification_prompt(retry_count: int) -> str:
    if retry_count <= 0:
        return "ახლა სთხოვეთ ვერიფიკაციის კოდის შეყვანა, რათა ზუსტი ბალანსის გამჟღავნება გაგრძელდეს."
    return "წინა ვერიფიკაცია ვერ დადასტურდა. სთხოვეთ აბონენტს, კლავიატურით თავიდან სცადოს."


def _submit_key_instruction() -> str:
    if DTMF_SUBMIT_KEY == "#":
        return "ბოლოს დააჭიროს დიეზს (#)."
    return f"ბოლოს დააჭიროს {DTMF_SUBMIT_KEY}-ს."


def _technical_identifier_tts_text(retry_count: int) -> str:
    if retry_count <= 0:
        return (
            "გთხოვთ, ტელეფონის კლავიატურით შეიყვანეთ თქვენი სააბონენტო ან ანგარიშის ნომერი "
            f"და {_submit_key_instruction()}"
        )
    if retry_count == 1:
        return (
            "ვერ მოხერხდა თქვენი ნომრის ამოცნობა. გთხოვთ, კიდევ ერთხელ შეიყვანეთ "
            "ტელეფონის კლავიატურით თქვენი სააბონენტო ან ანგარიშის ნომერი "
            f"და {_submit_key_instruction()}"
        )
    return (
        "ვწუხვარ, ისევ ვერ მოხერხდა ნომრის ამოცნობა. გთხოვთ, კიდევ ერთხელ სცადეთ "
        "ტელეფონის კლავიატურით თქვენი სააბონენტო ან ანგარიშის ნომრის შეყვანა "
        f"და {_submit_key_instruction()}"
    )


def _billing_identifier_tts_text(retry_count: int) -> str:
    if retry_count <= 0:
        return (
            "გთხოვთ, ტელეფონის კლავიატურით შეიყვანეთ თქვენი სააბონენტო ან ანგარიშის ნომერი, "
            f"რათა დაცულ ბილინგის დეტალებზე გადასვლა გაგრძელდეს, და {_submit_key_instruction()}"
        )
    return (
        "ვერ მოხერხდა თქვენი ნომრის ამოცნობა. გთხოვთ, კიდევ ერთხელ შეიყვანეთ "
        "ტელეფონის კლავიატურით თქვენი სააბონენტო ან ანგარიშის ნომერი "
        f"და {_submit_key_instruction()}"
    )


def _billing_verification_tts_text(retry_count: int) -> str:
    if retry_count <= 0:
        return (
            "გთხოვთ, ტელეფონის კლავიატურით შეიყვანეთ ვერიფიკაციის კოდი "
            f"და {_submit_key_instruction()}"
        )
    return (
        "წინა ვერიფიკაცია ვერ დადასტურდა. გთხოვთ, კიდევ ერთხელ შეიყვანეთ "
        f"ვერიფიკაციის კოდი და {_submit_key_instruction()}"
    )


def _callback_number_tts_text() -> str:
    return (
        "გთხოვთ, ტელეფონის კლავიატურით შეიყვანეთ სასურველი უკუგამოძახების ნომერი "
        f"და {_submit_key_instruction()}"
    )


def _developer_message(content: str) -> dict[str, str]:
    return {
        "role": "developer",
        "content": content,
    }


def _known_context_message(tool_runtime: PipecatToolRuntime) -> dict[str, str]:
    context = tool_runtime.conversation_context()
    prompt_context = _build_prompt_context_summary(context)
    if not prompt_context:
        summary = "ცნობილი ზარის კონტექსტი: არ არის."
    else:
        summary = (
            "განსაზღვრული ინსტრუმენტებიდან ცნობილი ზარის კონტექსტი: "
            + json.dumps(prompt_context, ensure_ascii=False, sort_keys=True)
        )
    return _developer_message(summary)


def _build_prompt_context_summary(context: dict[str, Any]) -> dict[str, Any]:
    if not context:
        return {}

    summary: dict[str, Any] = {}
    for key in _PROMPT_CONTEXT_FIELDS:
        compact_value = _compact_prompt_value(context.get(key))
        if compact_value is not None:
            summary[key] = compact_value

    technical_status = context.get("technical_status")
    compact_technical_status = _compact_prompt_mapping(technical_status, depth=0)
    if compact_technical_status:
        summary["technical_status"] = compact_technical_status

    if isinstance(context.get("lookup_context"), dict):
        summary["lookup_context_available"] = True

    return summary


def _compact_prompt_mapping(
    value: Any,
    *,
    depth: int,
) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None

    compact: dict[str, Any] = {}
    omitted = 0
    processed = 0
    for key in sorted(value):
        processed += 1
        compact_value = _compact_prompt_value(value[key], depth=depth + 1)
        if compact_value is None:
            omitted += 1
            continue
        compact[key] = compact_value
        if len(compact) >= _PROMPT_CONTEXT_SCALAR_LIMIT:
            omitted += max(0, len(value) - processed)
            break

    if omitted > 0:
        compact["_omitted_keys"] = omitted
    return compact or None


def _compact_prompt_value(value: Any, *, depth: int = 0) -> Any:
    if value is None:
        return None
    if isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        if len(normalized) <= _PROMPT_CONTEXT_STR_LIMIT:
            return normalized
        return normalized[: _PROMPT_CONTEXT_STR_LIMIT - 1].rstrip() + "..."
    if isinstance(value, list):
        compact_items = [
            item
            for item in (_compact_prompt_value(item, depth=depth + 1) for item in value[:4])
            if item is not None and not isinstance(item, dict)
        ]
        if compact_items:
            return compact_items
        return f"list[{len(value)}]"
    if isinstance(value, dict):
        if depth > 0:
            return None
        return _compact_prompt_mapping(value, depth=depth)
    return str(value)


def _clear_dtmf_collection_action() -> dict[str, Any]:
    return {"type": "clear_dtmf_collection"}


def _tts_say_action(text: str) -> dict[str, Any]:
    return {
        "type": "gateway_tts_say",
        "text": text,
    }


def _start_dtmf_collection_action(
    *,
    kind: str,
    on_complete: Callable[[str, Any], Awaitable[None]],
) -> dict[str, Any]:
    return {
        "type": "begin_dtmf_collection",
        "kind": kind,
        "submit_key": DTMF_SUBMIT_KEY,
        "on_complete": on_complete,
    }


def _technical_identifier_complete_handler(
    tool_runtime: PipecatToolRuntime,
    *,
    retry_count: int,
) -> Callable[[str, Any], Awaitable[None]]:
    async def _handler(value: str, flow_manager) -> None:
        flow_manager.state["branch"] = "technical_support"
        lookup_payload = await tool_runtime.execute_tool(
            tool_id=LOOKUP_CUSTOMER_TOOL_ID,
            arguments={"subscriber_identifier": value},
        )
        flow_manager.state["last_tool_id"] = LOOKUP_CUSTOMER_TOOL_ID
        flow_manager.state["last_tool_result"] = lookup_payload
        flow_manager.state["lookup_result"] = lookup_payload

        if _lookup_found(lookup_payload):
            context_payload = await tool_runtime.execute_tool(
                tool_id=LOAD_SERVICE_CONTEXT_TOOL_ID,
                arguments={},
            )
            flow_manager.state["last_tool_id"] = LOAD_SERVICE_CONTEXT_TOOL_ID
            flow_manager.state["last_tool_result"] = context_payload
            flow_manager.state["service_context_result"] = context_payload
            await flow_manager.set_node_from_config(
                build_technical_diagnosis_node(tool_runtime)
            )
            return

        if retry_count + 1 < IDENTIFIER_MAX_ATTEMPTS:
            await flow_manager.set_node_from_config(
                build_technical_support_node(
                    tool_runtime,
                    retry_count=retry_count + 1,
                )
            )
            return

        flow_manager.state["branch"] = "human_handoff"
        await flow_manager.set_node_from_config(build_human_handoff_node(tool_runtime))

    return _handler


def _billing_identifier_complete_handler(
    tool_runtime: PipecatToolRuntime,
    *,
    retry_count: int,
) -> Callable[[str, Any], Awaitable[None]]:
    async def _handler(value: str, flow_manager) -> None:
        flow_manager.state["branch"] = "billing"
        lookup_payload = await tool_runtime.execute_tool(
            tool_id=LOOKUP_CUSTOMER_TOOL_ID,
            arguments={"subscriber_identifier": value},
        )
        flow_manager.state["last_tool_id"] = LOOKUP_CUSTOMER_TOOL_ID
        flow_manager.state["last_tool_result"] = lookup_payload
        flow_manager.state["lookup_result"] = lookup_payload

        if _lookup_found(lookup_payload):
            await flow_manager.set_node_from_config(build_billing_verification_node(tool_runtime))
            return

        if retry_count + 1 < IDENTIFIER_MAX_ATTEMPTS:
            await flow_manager.set_node_from_config(
                build_billing_identifier_collection_node(
                    tool_runtime,
                    retry_count=retry_count + 1,
                )
            )
            return

        flow_manager.state["branch"] = "human_handoff"
        await flow_manager.set_node_from_config(build_human_handoff_node(tool_runtime))

    return _handler


def _billing_verification_complete_handler(
    tool_runtime: PipecatToolRuntime,
    *,
    retry_count: int,
) -> Callable[[str, Any], Awaitable[None]]:
    async def _handler(value: str, flow_manager) -> None:
        flow_manager.state["branch"] = "billing"
        verification_payload = await tool_runtime.execute_tool(
            tool_id=VERIFY_CUSTOMER_TOOL_ID,
            arguments={"subscriber_identifier": value},
        )
        flow_manager.state["last_tool_id"] = VERIFY_CUSTOMER_TOOL_ID
        flow_manager.state["last_tool_result"] = verification_payload
        flow_manager.state["verification_result"] = verification_payload

        if bool(verification_payload.get("verified")):
            await flow_manager.set_node_from_config(
                build_billing_exact_balance_node(tool_runtime)
            )
            return

        if retry_count + 1 < VERIFICATION_MAX_ATTEMPTS:
            await flow_manager.set_node_from_config(
                build_billing_verification_node(
                    tool_runtime,
                    retry_count=retry_count + 1,
                )
            )
            return

        flow_manager.state["branch"] = "human_handoff"
        await flow_manager.set_node_from_config(build_human_handoff_node(tool_runtime))

    return _handler


def _handoff_callback_complete_handler(
    tool_runtime: PipecatToolRuntime,
) -> Callable[[str, Any], Awaitable[None]]:
    async def _handler(value: str, flow_manager) -> None:
        tool_runtime.set_callback_number(value)
        flow_manager.state["branch"] = "human_handoff"
        flow_manager.state["callback_number"] = value
        await flow_manager.set_node_from_config(build_human_handoff_node(tool_runtime))

    return _handler


def _balance_verification_route_function(
    tool_runtime: PipecatToolRuntime,
) -> FlowsFunctionSchema:
    async def _handler(args: dict[str, Any], flow_manager) -> tuple[None, NodeConfig]:
        _ = args
        flow_manager.state["branch"] = "billing"
        if tool_runtime.has_customer_context():
            return None, build_billing_verification_node(tool_runtime)
        return None, build_billing_identifier_collection_node(tool_runtime)

    return FlowsFunctionSchema(
        name="route_to_balance_verification",
        description="Route exact-balance disclosure into the required keypad verification flow. If customer context is missing, identifier collection happens first.",
        properties={},
        required=[],
        handler=_handler,
    )


def _cached_balance_function(tool_runtime: PipecatToolRuntime) -> FlowsFunctionSchema:
    async def _handler(args: dict[str, Any], flow_manager) -> dict[str, Any]:
        _ = args
        payload = tool_runtime.cached_balance_payload()
        if not payload.get("verified"):
            result = {
                "error": "verification_required",
                "verified": False,
            }
        elif payload.get("balance") is None:
            result = {
                "error": "balance_unavailable",
                "verified": True,
                "account_id": payload.get("account_id"),
            }
        else:
            result = {
                "verified": True,
                "account_id": payload.get("account_id"),
                "balance": payload.get("balance"),
                "currency": payload.get("currency"),
            }
        flow_manager.state["last_tool_id"] = "get_cached_balance"
        flow_manager.state["last_tool_result"] = result
        return result

    return FlowsFunctionSchema(
        name="get_cached_balance",
        description="Return the exact cached balance after successful billing verification.",
        properties={},
        required=[],
        handler=_handler,
    )


def _end_call_function(tool_runtime: PipecatToolRuntime) -> FlowsFunctionSchema:
    async def _handler(args: dict[str, Any], flow_manager) -> dict[str, Any]:
        _ = args
        reason = "assistant_completed"
        flow_manager.state["pending_end_call"] = True
        flow_manager.state["end_call_reason"] = reason
        scheduled = tool_runtime.schedule_end_call(reason)
        return {
            "ending": scheduled,
            "reason": reason,
        }

    return FlowsFunctionSchema(
        name="end_call",
        description="Schedule the SIP hangup after your final spoken closing once the conversation is complete.",
        properties={},
        required=[],
        handler=_handler,
    )


def _lookup_found(payload: dict[str, Any]) -> bool:
    return bool(payload.get("found")) and payload.get("lookup_status") == "found"


def _route_function(
    *,
    name: str,
    description: str,
    next_node_factory: Callable[[], NodeConfig],
    branch_name: str,
) -> FlowsFunctionSchema:
    async def _handler(args: dict[str, Any], flow_manager) -> tuple[None, NodeConfig]:
        _ = args
        flow_manager.state["branch"] = branch_name
        return None, next_node_factory()

    return FlowsFunctionSchema(
        name=name,
        description=description,
        properties={},
        required=[],
        handler=_handler,
    )


def _tool_function(
    tool_runtime: PipecatToolRuntime,
    tool_id: str,
) -> FlowsFunctionSchema:
    entry = tool_runtime.tool_entry(tool_id)
    schema = tool_runtime.function_schema(tool_id)

    async def _handler(args: dict[str, Any], flow_manager) -> dict[str, Any]:
        flow_manager.state["last_tool_id"] = tool_id
        payload = await tool_runtime.execute_tool(tool_id=tool_id, arguments=args)
        flow_manager.state["last_tool_result"] = payload
        return payload

    return FlowsFunctionSchema(
        name=schema.name,
        description=schema.description,
        properties=schema.properties,
        required=schema.required,
        handler=_handler,
        timeout_secs=entry.timeout_s,
    )
