import json
import os
from pathlib import Path
from random import Random

from dotenv import load_dotenv


load_dotenv(Path(__file__).with_name(".env"))
output_path = Path(os.environ["INPUT_PATH"])
user_count = int(os.environ["USER_COUNT"])
purchases_per_user = int(os.environ["PURCHASES_PER_USER"])

if user_count < 1 or purchases_per_user < 1:
    raise ValueError("USER_COUNT and PURCHASES_PER_USER must be positive")
if output_path.resolve() == (Path(__file__).parent / "data/purchases.jsonl").resolve():
    raise ValueError("INPUT_PATH must not overwrite data/purchases.jsonl")

output_path.parent.mkdir(parents=True, exist_ok=True)
rng = Random(0)

with output_path.open("w") as output:
    for purchase_index in range(purchases_per_user):
        for user_id in range(1, user_count + 1):
            event = {
                "event_id": purchase_index * user_count + user_id,
                "user_id": user_id,
                "amount_cents": rng.randint(100, 10_000),
            }
            output.write(json.dumps(event) + "\n")
