from typing import Any

from apache_beam.pipeline import Pipeline
from apache_beam.transforms.ptransform import InputT, OutputT
import apache_beam as beam
import logfire

logfire.configure()


class AutoSpanDoFn(beam.DoFn):
    def __init__(self, transform: beam.PTransform[InputT, OutputT]):
        super().__init__()
        self.transform = transform

    def process(self, element: Any, *args, **kwargs):
        label = self.transform.label if hasattr(self.transform, "label") else "UnnamedTransform"
        with logfire.span(label):
            yield element


class AutoSpanTransform(beam.PTransform[InputT, OutputT]):
    def __init__(self, transform: beam.PTransform[InputT, OutputT]):
        super().__init__()
        self.transform = transform

    def expand(self, input_or_inputs: InputT) -> OutputT:
        return input_or_inputs | beam.ParDo(AutoSpanDoFn(self.transform)) | self.transform


class Split(beam.DoFn):
    def process(self, element: str):
        return element.split(" ")


def logfire_print(element: str):
    logfire.info(element)


with logfire.span("main"):
    with Pipeline() as pipeline:
        text = [
            "To be, or not to be: that is the question: ",
            "Whether 'tis nobler in the mind to suffer ",
            "The slings and arrows of outrageous fortune, ",
            "Or to take arms against a sea of troubles, ",
        ]

        pipeline = (
            pipeline
            | "Create" >> beam.Create(text)
            | "Split" >> AutoSpanTransform(beam.ParDo(Split()))
            | "Filter" >> AutoSpanTransform(beam.Filter(lambda x: x != "the"))
            | "Print" >> AutoSpanTransform(beam.Map(logfire_print))
        )
