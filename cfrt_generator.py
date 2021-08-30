from gumby.modules.experiment_module import ExperimentModule, static_module


@static_module
class CfrtScenarioGenerator(ExperimentModule):

    def __init__(self, experiment):
        super(CfrtScenarioGenerator, self).__init__(experiment)
        self.experiment.scenario_runner.preprocessor_callbacks["cfrt_generate"] = self._preproc_cfrt_generate
        self.generated_lines = {}

    def add_line(self, name, line):
        if name not in self.generated_lines:
            self.generated_lines[name] = 1
        self.experiment.scenario_runner.line_buffer.append(("<generator " + name + ">", self.generated_lines[name], line))
        self.generated_lines[name] += 1

    def _preproc_cfrt_generate(self, filename, line_number, line):
        # phase 1: parse generator instructions/vars
        unnamed_args, named_args = self.experiment.scenario_runner._parse_arguments(line)
        name = "cfrt_generator"
        if "name" in named_args:
            name = named_args["name"]

        # phase 2: generate
        for x in range(int(named_args["start"]), int(named_args["stop"]), int(named_args["step"])):
            self.add_line(name, "@0:%s cfrt_add_data_item {1} " % x)
        pass

