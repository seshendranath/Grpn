### Spark Submit Task in Zombie Runner (an ETL scheduling tool)

import os, itertools, yaml
from task import Base
from ..shared import sideput, os_util, yaml_util


__author__ = 'aguyyala'



class SparkSubmit(Base):

    """
    Submits a standalone spark job to a cluster with the provided options.
    A sample SparkSubmit task
        my_spark_job:
          class: SparkSubmit
          emits: foo
          configuration:
            spark-home: # optional field fallbacks on SPARK_HOME value set in context
            config-file:
            app-config:
                application: my_spark_application.py
                params:
                   param1: value1
                   param2: value2
                resources:
                   pyfiles:
                        - module1.py
                        - module2.py
                   jars:
                        - library1.jar
                        - library2.jar
                   files:
                        - myfile1.txt
                        - myfile2.txt
            cluster-config:
                  master: spark://spark-master:7070
                  deploy-mode: client
                  queue: default
            other-config:
                  dummy_option2: option2
                  dummy_option1: option1
      spark-home:
            The spark-home node is optional is used to get the SPARK_HOME directory.
            SPARK_HOME can be deduced on three layers on the following order
            i) spark-home node
           ii) SPARK_HOME context variable either defined in ZRC2 file or in the context section
          iii) SPARK_HOME environment variable
      config-file:
            have your frequently used cluster-config and other-config settings in this reusable config
            file which will be merged with task-inline settings for cluster-config and other-config.
      This also supports emits functionality in which the output of the driver is parsed as json and merged
      with jobs configuration.
       app-config:
           this node defines the application part of the setting
            application: the main application. This can be a packaged jar file or a python module
            params: this node contains parameters to be passed to your application.
               If all parameter are named arguments they provide a Yaml map object.
               for eg: params:
                           param1: value1
                           param2: value2
                       which will translate to --param1 value1 --param2 value2
                if all or some parameters are positional use an array.
                        param:
                           - value1
                           - --param2
                           - value2
                or else you can pass a single text node with each parameter split by a blank space
            resources:
                Use this node to pass additional resources to your application. This can either be Jars for your
                JVM app or other python modules for your python app or can simply be files used as resources.
                To support all these use pyfiles, jars or files.
        cluster-config:
             Any config specific to the cluster goes under here. The only restriction being it must always be a named
             argument parameter. ie a map in yaml terms. most likely candidates are --master, --deploy-mode, --queue etc.
        other-config:
             Any other additional config. same as cluster-config but if you dont want to include certain configs in cluster-config
             for brevity reasons, it can be included here.
    """
    SPARK_HOME = "SPARK_HOME"

    @staticmethod
    def _flat_node_to_cmd_line_args(node):
        """
            takes a parsed yaml node and translates it to a list of arguments that can be passed any sub-process command.
            for dictionary values the values can only be of type str or list. nested dictionary is not supported.
        :param node: the parsed yaml node which will either be a dict, list or string
        :return: list of arguments which either be positional or named.
        """
        if isinstance(node, list):
            return node
        elif isinstance(node, dict):
            return list(itertools.chain(*[['--%s' % key,node[key]] if
                    isinstance(node[key],basestring)  else ['--%s' % key] + node[key] for key in node.keys()]))
        elif isinstance(node, basestring):
            return node.split()
        else:
            raise ValueError("%s node is has unsupported data type")



    def _environment(self):
        """
        parses all inputs for the task and compose the spark submit command
        :return: None
        """

        self.spark_home = self._config_default("spark-home",
                        self._context(SparkSubmit.SPARK_HOME, default = os.environ.get(SparkSubmit.SPARK_HOME,None)))
        assert self.spark_home, "unable to detect SPARK_HOME. set SPARK_HOME as directed in the task documentation"
        assert os.path.exists(self.spark_home), "provided SPARK_HOME doesn't exists"

        spark_config = {'cluster-config': {}, 'other-config': {}}
        if 'config-file' in self._config_keys():
            spark_config.update(yaml.load(open(self._config('config-file')))['spark-config'])

        self.app_config = []

        spark_app = self._config('app-config')
        self.app_config.append(spark_app['application'])
        app_params = SparkSubmit._flat_node_to_cmd_line_args(spark_app['params']) if 'params' in spark_app else []
        self.app_config.extend(app_params)
        if 'resources' in spark_app:
            resources = [ ['--%s' % item] + (spark_app['resources'][item]) for item in spark_app['resources'].keys() ]
            self.resources = list(itertools.chain(*resources))
        else:
            self.resources = []


        cluster_config = self._config_default('cluster-config', {})
        cluster_config.update(spark_config['cluster-config'])
        self.cluster_options = list(itertools.chain(*[ ['--%s' % item, str(cluster_config[item]) ] for item in cluster_config.keys() ]))


        ##other options
        ## cluster options
        other_options = self._config_default('other-config',{})
        cluster_config.update(spark_config['other-config'])
        self.other_options = list(itertools.chain(*[ ['--%s' % item, str(other_options[item]) ] for item in other_options.keys() ]))


    def _work(self):
        """
        execute the spark-submit command
        :return: None
        """
        command = [ os.path.join(self.spark_home,'bin/spark-submit') ] + self.cluster_options + self.other_options + \
                  self.resources + self.app_config

        sideput.sideput( "spark submit command is %s" % ' '.join(command) )

        with sideput.Timing("spark job completed in %d seconds"):
            result, stdout, stderr = os_util.execute_command(command, do_sideput=True)

        sideput.sideput("[%s] stderr:\n%s" % (self.name(), stderr), level="INFO")
        sideput.sideput("[%s] stdout:\n%s" % (self.name(), stdout), level="INFO")
        if result != 0:
            raise Exception("spark job failed with code %d" % result)
        else:
            try:
                result_hash = yaml_util.load(stdout) if self._emits() else {}
                sideput.sideput("parsed stdout is %s\n" % result_hash, level="INFO")
            except Exception as e:
                result_hash = {}
                sideput.sideput("parsing stdout as json failed with message %s \n" % e.message , level= "ERROR")
                sideput.sideput("stdout is \n %s \n" % stdout, level="ERROR")
                raise e
            sideput.sideput("[%s] spark job completed successfully"
                             % self.name(), level = "INFO")
        return result_hash




if __name__ == '__main__':
    raise NotImplementedError("cannot be invoked as a main script")


