from datetime import datetime as dt, date, time

from typing import Optional, Annotated
from beartype import beartype
from beartype.vale import Is

from googleapiclient.discovery import build

from ..auth.authenticate import Authenticator
from ..utils.watermark import _is_rfc3339

@beartype
class GForms():
    def __init__(self, authenticator: Authenticator):
        self.authenticator = authenticator


    def get_forms_service(self):
        creds = self.authenticator.authenticate()
        return build('forms', 'v1', credentials=creds)

    def fetch_responses(self, form_id: str, after_watermark: Optional[Annotated[str, Is[_is_rfc3339]]] = None) -> list:
        service = self.get_forms_service()

        params = {}
        if after_watermark:
            # iso = after.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
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

    def get_question_id_title_map(self, form_id: str):
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

        # print(form.get("items", []))  # Debugging output
        return id_to_title
    
    def extract_form_data(self, form_id: str, include_fields: list = [], after_watermark: Optional[Annotated[str, Is[_is_rfc3339]]] = None):
        responses = self.fetch_responses(form_id, after_watermark=after_watermark)
        question_map = self.get_question_id_title_map(form_id)
        rows = []

        for res in responses:
            base_row = {
                "response_id": res.get("responseId"),
                "create_time": res.get("createTime"),
                "submit_time": res.get("lastSubmittedTime"),
            }

            if include_fields:
                answers = res.get("answers", {})
                for qid, answer in answers.items():
                    row = base_row.copy()

                    # Only add fields explicitly mentioned in include_fields
                    if "question_id" in include_fields:
                        row["question_id"] = qid

                    if "question_title" in include_fields:
                        row["question_title"] = question_map.get(qid, qid)

                    # if "textAnswers" in answer and "textAnswers" in include_fields:
                    #     text_list = answer["textAnswers"].get("answers", [])
                    #     row["answer"] = text_list[0].get("value") if text_list else None

                    if "textAnswers" in answer and "textAnswers" in include_fields:
                        text_list = answer["textAnswers"].get("answers", [])
                        row["answer"] = ", ".join([x.get("value", "") for x in text_list]) if text_list else None



                    if "fileUploadAnswers" in answer and "fileUploadAnswers" in include_fields:
                        files = answer["fileUploadAnswers"].get("answers", [])
                        row["fileIds"] = [f.get("fileId") for f in files]
                        row["fileNames"] = [f.get("fileName") for f in files]
                        row["mimeTypes"] = [f.get("mimeType") for f in files]

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
            else:
                rows.append(base_row)

        return rows
    
    def extract_answer_value(self, answer, include_fields=None):
        """
        Helper function to extract the answer value(s) based on type.
        For 'wide' format, just returns a display value for the cell.
        For 'long' format, fill dict fields as needed.
        """
        if not answer:
            return ""
        # if "textAnswers" in answer:
        #     vals = [a.get("value", "") for a in answer["textAnswers"].get("answers", [])]
        #     return "\n".join(vals)
        
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


    def extract_form_data_new(self, form_id: str, include_fields: list = [], format: str = "long"):
        responses = self.fetch_responses(form_id)
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
            return rows

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
                    row[qtext] = self.extract_answer_value(answer)
                rows.append(row)
            return rows

        else:
            raise ValueError("Unknown format: must be 'long' or 'wide'")
