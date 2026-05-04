import dataclasses
from dataclasses import dataclass
from datetime import UTC, datetime
import random
import json

@dataclass(slots=True)
class VitalEvent:
    subject_id: str
    device_id: int
    event_time: int
    heart_rate: int
    oxygen_level: int
    systolic_bp: int
    diastolic_bp: int
    body_temperature: float
    activity_level: str
    alert_flag: bool
    
def _sample_activity_level() -> str:
    return random.choices(
        population=["sleeping", "resting", "walking", "running"],
        weights=[0.15, 0.45, 0.30, 0.10],
        k=1,
    )[0]


def _base_vitals(activity_level: str) -> tuple[int, int, int, int, float]:
    if activity_level == "sleeping":
        return (
            random.randint(55, 70),
            random.randint(95, 100),
            random.randint(95, 115),
            random.randint(60, 75),
            round(random.uniform(36.1, 36.8), 1),
        )
    if activity_level == "resting":
        return (
            random.randint(65, 85),
            random.randint(95, 100),
            random.randint(105, 125),
            random.randint(68, 82),
            round(random.uniform(36.3, 37.0), 1),
        )
    if activity_level == "walking":
        return (
            random.randint(80, 105),
            random.randint(94, 99),
            random.randint(110, 135),
            random.randint(70, 88),
            round(random.uniform(36.5, 37.3), 1),
        )
    return (
        random.randint(95, 135),
        random.randint(92, 98),
        random.randint(120, 150),
        random.randint(78, 95),
        round(random.uniform(36.8, 38.0), 1),
    )


def _inject_outlier(
    heart_rate: int,
    oxygen_level: int,
    systolic_bp: int,
    diastolic_bp: int,
    body_temperature: float,
) -> tuple[int, int, int, int, float]:
    if random.random() > 0.10:
        return heart_rate, oxygen_level, systolic_bp, diastolic_bp, body_temperature

    anomaly = random.choice(["tachycardia", "hypoxia", "hypertension", "fever"])
    if anomaly == "tachycardia":
        heart_rate = random.randint(121, 150)
    elif anomaly == "hypoxia":
        oxygen_level = random.randint(88, 91)
    elif anomaly == "hypertension":
        systolic_bp = random.randint(161, 185)
        diastolic_bp = random.randint(101, 120)
    else:
        body_temperature = round(random.uniform(38.1, 39.5), 1)

    return heart_rate, oxygen_level, systolic_bp, diastolic_bp, body_temperature


def _compute_alert_flag(event: VitalEvent) -> bool:
    return any(
        [
            event.heart_rate > 120,
            event.oxygen_level < 92,
            event.systolic_bp > 160,
            event.diastolic_bp > 100,
            event.body_temperature > 38.0,
        ]
    )


def generate_vital_event(subject_id: str, device_id: int) -> VitalEvent:
    activity_level = _sample_activity_level()
    heart_rate, oxygen_level, systolic_bp, diastolic_bp, body_temperature = _base_vitals(activity_level)
    heart_rate, oxygen_level, systolic_bp, diastolic_bp, body_temperature = _inject_outlier(
        heart_rate,
        oxygen_level,
        systolic_bp,
        diastolic_bp,
        body_temperature,
    )

    event = VitalEvent(
        subject_id=str(subject_id),
        device_id=int(device_id),
        event_time=int(datetime.now(UTC).timestamp() * 1000),
        heart_rate=int(heart_rate),
        oxygen_level=int(oxygen_level),
        systolic_bp=int(systolic_bp),
        diastolic_bp=int(diastolic_bp),
        body_temperature=float(body_temperature),
        activity_level=str(activity_level),
        alert_flag=bool(False),
    )
    event.alert_flag = bool(_compute_alert_flag(event))
    return event

def event_serializer(event: VitalEvent) -> bytes:
    event_dict = dataclasses.asdict(event)
    event_json = json.dumps(event_dict).encode("utf-8")
    return event_json

def event_deserializer(v: bytes) -> VitalEvent:
    event_json = v.decode("utf-8")
    event_dict = json.loads(event_json)
    return VitalEvent(**event_dict)