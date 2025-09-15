from datetime import datetime as dt, date, time

from typing import Optional, Annotated, Literal, Union
from beartype import beartype
from beartype.vale import Is
from pathlib import Path
import json
import pandas as pd
import numpy as np
import pyspark

from googleapiclient.discovery import build

from ..auth.authenticate import Authenticator
from ..utils.watermark import _is_rfc3339

@beartype
class GForms():
    def __init__(self, authenticator: Authenticator, idmap_loc: str = None):
        self.authenticator = authenticator

        if idmap_loc is not None:
            with open(idmap_loc, 'r') as f:
                self.idmap = json.load(f)

    def get_forms_service(self):
        """
        helper function to get the Google Forms API service resource.
        """
        creds = self.authenticator.authenticate()
        return build('forms', 'v1', credentials=creds)

    def fetch_responses(self, form_id: str = None, form_name: str = None, after_watermark: Optional[Annotated[str, Is[_is_rfc3339]]] = None) -> list:
        """
        Fetch all responses for a given Google Form.

        Args:
            form_id (str): The ID of the Google Form.
            after_watermark (Optional[str]): RFC3339 timestamp string to filter responses after this time.

        Returns:
            list: List of response objects from the form.
        """

        if (form_id is None) and (form_name is None):
            raise ValueError("Either form_id or form_name must be provided.")

        if form_name:
            form_id = self.idmap.get(form_name)

        service = self.get_forms_service()

        params = {}
        if after_watermark:
            params["filter"] = f"timestamp > {after_watermark}"

        all_responses = []
        page_token = None

        while True:
            request = service.forms().responses().list(formId=form_id, pageToken=page_token, **params)
            response = request.execute()

            all_responses.extend(response.get('responses', []))
            page_token = response.get('nextPageToken')

            if not page_token:
                break

        return all_responses
    
    def extract_answer_value(self, answer):
        """
        Helper function to extract the answer value(s) based on type.
        """
        if not answer:
            return ""
        if "textAnswers" in answer:
            vals = [a.get("value", "") for a in answer["textAnswers"].get("answers", [])]
            return ", ".join(vals)

        if "fileUploadAnswers" in answer:
            vals = [f.get("fileName", "") for f in answer["fileUploadAnswers"].get("answers", [])]
            return ", ".join(vals)
        if "dateAnswers" in answer:
            vals = [date(d["year"], d["month"], d["day"]).isoformat()
                    for d in answer["dateAnswers"].get("answers", [])
                    if all(k in d for k in ("year", "month", "day"))]
            return ", ".join(vals)
        if "timeAnswers" in answer:
            vals = [time(t["hours"], t["minutes"]).strftime("%H:%M")
                    for t in answer["timeAnswers"].get("answers", [])
                    if t.get("hours") is not None and t.get("minutes") is not None]
            return ", ".join(vals)
        return ""

    def get_question_id_title_map(self, form_id: str):
        """
        Helper function to get Mapping from questionId to question title.
        """
        service = self.get_forms_service()
        form = service.forms().get(formId=form_id).execute()

        id_to_title = {}
        for item in form.get("items", []):
            try:
                qid = item["questionItem"]["question"]["questionId"]
                title = item.get("title", "").strip()
                id_to_title[qid] = title
            except KeyError:
                continue  # Skip non-question items

        return id_to_title

    def extract_form_data(self, form_id: str = None, form_name: str = None, include_fields: list = ['question_id', 'question_title', 'textAnswers', 'fileUploadAnswers', 'dateAnswers', 'timeAnswers'], format: Literal['long', 'wide'] = "wide", after_watermark: Optional[Annotated[str, Is[_is_rfc3339]]] = None, rtype: Literal['pandas', 'pyspark', 'list'] = 'list') -> Union[list, pd.DataFrame, pyspark.sql.DataFrame]:
        """
        Extract form response data in either 'long' or 'wide' format.

        Args:
            form_id (str): The ID of the Google Form.
            include_fields (list): List of fields to include in the output rows.
            format (Literal['long', 'wide']): Output format. 'long' for row per answer, 'wide' for row per response.
            after_watermark (Optional[str]): RFC3339 timestamp string to filter responses after this time.

        Returns:
            list: List of dictionaries representing extracted response data.
        """

        if (form_id is None) and (form_name is None):
            raise ValueError("Either form_id or form_name must be provided.")

        if form_name in self.idmap.keys():
            form_id = self.idmap.get(form_name)
        else: 
            raise ValueError(f"Form name '{form_name}' not found in idmap.")

        responses = self.fetch_responses(form_id, after_watermark=after_watermark)
        question_map = self.get_question_id_title_map(form_id)
        question_ids = list(question_map.keys())
        question_titles = [question_map[qid] for qid in question_ids]
        rows = []

        if format == "long":
            for res in responses:
                base_row = {
                    "response_id": res.get("responseId"),
                    "create_time": res.get("createTime"),
                    "submit_time": res.get("lastSubmittedTime"),
                }
                answers = res.get("answers", {})
                for qid, answer in answers.items():
                    row = base_row.copy()
                    # Only add fields explicitly mentioned in include_fields
                    if "question_id" in include_fields:
                        row["question_id"] = qid
                    if "question_title" in include_fields:
                        row["question_title"] = question_map.get(qid, qid)
                    # Reuse value extraction logic
                    if "textAnswers" in answer and "textAnswers" in include_fields:
                        row["answer"] = self.extract_answer_value(answer)
                    if "fileUploadAnswers" in answer and "fileUploadAnswers" in include_fields:
                        row["fileIds"] = [f.get("fileId") for f in answer["fileUploadAnswers"].get("answers", [])]
                        row["fileNames"] = [f.get("fileName") for f in answer["fileUploadAnswers"].get("answers", [])]
                        row["mimeTypes"] = [f.get("mimeType") for f in answer["fileUploadAnswers"].get("answers", [])]
                    if "dateAnswers" in answer and "dateAnswers" in include_fields:
                        dates = answer["dateAnswers"].get("answers", [])
                        if dates:
                            d = dates[0]
                            if all(k in d for k in ("year", "month", "day")):
                                row["answer_date"] = dt.date(d["year"], d["month"], d["day"]).isoformat()
                    if "timeAnswers" in answer and "timeAnswers" in include_fields:
                        times = answer["timeAnswers"].get("answers", [])
                        if times:
                            t = times[0]
                            if t.get("hours") is not None and t.get("minutes") is not None:
                                row["answer_time"] = dt.time(t["hours"], t["minutes"]).strftime("%H:%M")
                    rows.append(row)

        elif format == "wide":
            for res in responses:
                row = {
                    "response_id": res.get("responseId"),
                    "create_time": res.get("createTime"),
                    "submit_time": res.get("lastSubmittedTime"),
                }
                answers = res.get("answers", {})
                for qid, qtext in zip(question_ids, question_titles):
                    answer = answers.get(qid, {})
                    # Reuse the same extraction logic
                    if "fileUploadAnswers" in answer and "fileUploadAnswers" in include_fields:
                        row[f"{qtext}_fileIds"] = [f.get("fileId") for f in answer["fileUploadAnswers"].get("answers", [])]
                        row[f"{qtext}_fileNames"] = [f.get("fileName") for f in answer["fileUploadAnswers"].get("answers", [])]
                        row[f"{qtext}_mimeTypes"] = [f.get("mimeType") for f in answer["fileUploadAnswers"].get("answers", [])]
                    else:
                        row[qtext] = self.extract_answer_value(answer)
                rows.append(row)

        if rtype == 'pandas':
            return pd.DataFrame(rows)
        elif rtype == 'pyspark':
            spark = pyspark.sql.SparkSession.builder.getOrCreate()
            return spark.createDataFrame(rows)
        else:
            return rows
