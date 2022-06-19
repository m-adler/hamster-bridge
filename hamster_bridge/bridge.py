import configparser
import logging
import os
import stat
import time

from hamster.lib.fact import Fact

logger = logging.getLogger(__name__)


try:
    import hamster.client
except ImportError:
    raise ImportError("Can not find hamster")


def _combine_configs(*configs):
    """
    Combines all configs (instances of RawConfigParser) into a single one
    containing all sections and values. If duplicates appear the later configs
    will overwrite the earlier values.
    Returns a RawConfigParser() instance.
    """
    result = configparser.RawConfigParser()
    for config in configs:
        for section in config.sections():
            try:
                result.add_section(section)
            except configparser.DuplicateSectionError:
                # Ignore it. We simply want to include all sections from
                # the source configs
                pass
            for option in config.options(section):
                value = config.get(section, option)
                result.set(section, option, value)
    return result


class HamsterBridge(hamster.client.Storage):
    """
    Connects to the running hamster instance via dbus. But as the notification does not work reliable there is a
    polling-based loop in the run()-method that will trigger all registered listeners.
    """

    def __init__(self, save_passwords=False):
        super(HamsterBridge, self).__init__()
        self._listeners = []
        self.save_passwords = save_passwords
        self._facts_cache: dict[int, Fact] = dict()

    def add_listener(self, listener):
        """
        Registers the given HamsterListener instance. It will then be notified about changes.

        :param listener: the HamsterListener instance
        :type  listener: HamsterListener
        """
        if listener not in self._listeners:
            self._listeners.append(listener)

    def configure(self, config_path):
        """
        Gives each listener the chance to do something before we start the bridge's runtime loops.
        :param config_path: path to config file
        :type config_path:  str
        """
        path = os.path.expanduser(config_path)
        config = configparser.RawConfigParser()
        sensitive_config = configparser.RawConfigParser()
        # read from file if exists
        if os.path.exists(path):
            logger.debug("Reading config file from %s", path)
            config.read(path)
        # let listeners extend
        for listener in self._listeners:
            logger.debug("Configuring listener %s", listener)
            listener.configure(config, sensitive_config)
        # save to file
        with open(path, "w") as configfile:
            logger.debug("Writing back configuration to %s", path)
            if self.save_passwords:
                all_configs = _combine_configs(config, sensitive_config)
                all_configs.write(configfile)
            else:
                config.write(configfile)
        # as we store passwords in clear text, let's at least set correct file permissions
        logger.debug("Setting owner only file permissions to %s", path)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)

    def run(self, polling_intervall=1):
        """
        Starts the polling loop that will run until receive common exit signals.

        :param polling_intervall: how often the connector polls data from haster in seconds (default: 1)
        :type  polling_intervall: int
        """
        for listener in self._listeners:
            logger.debug("Preparing listener %s", listener)
            listener.prepare()
        logger.info("Start listening for hamster activity...")

        self._facts_cache = {fact.id: fact for fact in self.get_todays_facts()}
        try:
            while True:
                self.process()
                time.sleep(polling_intervall)
        except (KeyboardInterrupt, SystemExit):
            pass

    def cleanup_extraneous_facts_cache_entries(self, current_facts: list[Fact]):
        extraneous_fact_ids = set(self._facts_cache.keys()) - {
            fact.id for fact in current_facts
        }
        for id in extraneous_fact_ids:
            del self._facts_cache[id]

    def process(self):
        current_facts = self.get_todays_facts()
        self.cleanup_extraneous_facts_cache_entries(current_facts)

        for fact in current_facts:
            cached_fact = self._facts_cache.get(fact.id)
            # Skip if ther are no changes
            if cached_fact == fact:
                continue

            if cached_fact is None:
                logger.debug("Found a new task: %r", vars(fact))
                for listener in self._listeners:
                    listener.on_fact_started(fact)

            is_cached_fact_started = not (cached_fact and cached_fact.end_time)
            if fact.end_time and is_cached_fact_started:
                logger.debug("Found a stopped task: %r", vars(fact))
                for listener in self._listeners:
                    listener.on_fact_stopped(fact)

            self._facts_cache[fact.id] = fact
