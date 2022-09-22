package streams.tool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.utils.CommandLineUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import scala.collection.JavaConverters;

/** */
public class RemoveGroupMembersTool {
  private static final int EXIT_CODE_SUCCESS = 0;
  private static final int EXIT_CODE_ERROR = 1;

  private static OptionSpec<String> bootstrapServerOption;
  private static OptionSpec<String> applicationIdOption;
  private static OptionSpec<String> prefixKeepOption;
  private static OptionSpec<String> instanceIdOption;
  private static OptionSpec<Void> helpOption;
  private static OptionSpec<Void> versionOption;
  private static OptionSpec<String> commandConfigOption;
  private static OptionSpec<Boolean> executeOption;

  private static final String USAGE =
      "This tool helps to quickly remove instances from a running Kafka Streams app.";

  private static final Logger LOGGER = Logger.getLogger(RemoveGroupMembersTool.class.getName());

  private OptionSet options = null;

  public int run(final String[] args) {
    return run(args, new Properties());
  }

  public int run(final String[] args, final Properties config) {
    int exitCode;

    Admin adminClient = null;
    try {
      parseArguments(args);

      final String groupId = options.valueOf(applicationIdOption);
      final List<String> instanceIds = options.valuesOf(instanceIdOption);

      final Properties properties = new Properties();
      if (options.has(commandConfigOption)) {
        properties.putAll(Utils.loadProps(options.valueOf(commandConfigOption)));
      }
      properties.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServerOption));

      adminClient = Admin.create(properties);

      if (options.valueOf(prefixKeepOption) != null) {
        exitCode =
            deleteAllButPrefix(
                groupId,
                adminClient,
                options.valueOf(prefixKeepOption),
                options.valueOf(executeOption));
      } else if (options.valueOf(instanceIdOption) != null) {
        exitCode =
            deleteActiveConsumers(
                groupId, adminClient, instanceIds, options.valueOf(executeOption));
      } else {
        exitCode = showActiveConsumers(groupId, adminClient);
      }
    } catch (final Throwable e) {
      exitCode = EXIT_CODE_ERROR;
      System.err.println("ERROR: " + e);
      e.printStackTrace(System.err);
    } finally {
      if (adminClient != null) {
        adminClient.close(Duration.ofSeconds(60));
      }
    }

    return exitCode;
  }

  protected int deleteAllButPrefix(
      final String groupId, final Admin adminClient, final String prefix, boolean execute)
      throws ExecutionException, InterruptedException {

    if (!execute) {
      LOGGER.warning("Executing in dry-run mode.");
    }

    final DescribeConsumerGroupsResult describeResult =
        adminClient.describeConsumerGroups(
            Collections.singleton(groupId),
            new DescribeConsumerGroupsOptions().timeoutMs(10 * 1000));
    final List<MemberDescription> members =
        new ArrayList<>(describeResult.describedGroups().get(groupId).get().members());

    final List<String> staticMembers =
        members.stream()
            .filter(memberDescription -> memberDescription.groupInstanceId().isPresent())
            .map(memberDescription -> memberDescription.groupInstanceId().get())
            .collect(Collectors.toList());

    List<String> dontFitPrefix =
        staticMembers.stream().filter(id -> !id.startsWith(prefix)).collect(Collectors.toList());

    if (!dontFitPrefix.isEmpty()) {
      LOGGER.warning(
          String.format(
              "Removing following members: %s  from application %s",
              dontFitPrefix.toString(), groupId));
      if (execute) {
        Set<MemberToRemove> membersToRemove =
            dontFitPrefix.stream().map(s -> new MemberToRemove(s)).collect(Collectors.toSet());
        adminClient
            .removeMembersFromConsumerGroup(
                groupId, new RemoveMembersFromConsumerGroupOptions(membersToRemove))
            .all()
            .get();
      }
    } else {
      LOGGER.warning(
          String.format("All the active instances start with %s. Nothing to delete.", prefix));
    }
    return 0;
  }

  protected int deleteActiveConsumers(
      final String groupId,
      final Admin adminClient,
      final List<String> instanceIds,
      boolean execute)
      throws ExecutionException, InterruptedException {

    if (!execute) {
      LOGGER.warning("Executing in dry-run mode.");
    }

    final DescribeConsumerGroupsResult describeResult =
        adminClient.describeConsumerGroups(
            Collections.singleton(groupId),
            new DescribeConsumerGroupsOptions().timeoutMs(10 * 1000));
    final List<MemberDescription> members =
        new ArrayList<>(describeResult.describedGroups().get(groupId).get().members());

    final List<String> staticMembers =
        members.stream()
            .filter(memberDescription -> memberDescription.groupInstanceId().isPresent())
            .map(memberDescription -> memberDescription.groupInstanceId().get())
            .collect(Collectors.toList());
    final List<String> instancesPresent =
        staticMembers.stream().filter(id -> instanceIds.contains(id)).collect(Collectors.toList());

    final List<String> presentInstanceIds =
        instanceIds.stream()
            .filter(instanceIdToRemove -> instancesPresent.contains(instanceIdToRemove))
            .collect(Collectors.toList());

    if (!presentInstanceIds.isEmpty()) {
      LOGGER.warning(
          String.format(
              "Removing following members: %s  from application %s",
              presentInstanceIds.toString(), groupId));
      if (execute) {
        Set<MemberToRemove> membersToRemove =
            presentInstanceIds.stream().map(s -> new MemberToRemove(s)).collect(Collectors.toSet());
        adminClient
            .removeMembersFromConsumerGroup(
                groupId, new RemoveMembersFromConsumerGroupOptions(membersToRemove))
            .all()
            .get();
      }
    } else {
      LOGGER.warning(String.format("None of the instances to remove are active."));
    }
    return 0;
  }

  protected int showActiveConsumers(final String groupId, final Admin adminClient)
      throws ExecutionException, InterruptedException {

    final DescribeConsumerGroupsResult describeResult =
        adminClient.describeConsumerGroups(
            Collections.singleton(groupId),
            new DescribeConsumerGroupsOptions().timeoutMs(10 * 1000));
    final List<MemberDescription> members =
        new ArrayList<>(describeResult.describedGroups().get(groupId).get().members());

    final List<String> staticMembers =
        members.stream()
            .filter(memberDescription -> memberDescription.groupInstanceId().isPresent())
            .map(memberDescription -> memberDescription.groupInstanceId().get())
            .collect(Collectors.toList());

    LOGGER.warning(String.format("Current members: %s", staticMembers.toString()));

    return 0;
  }

  private <T> void checkInvalidArgs(
      final OptionParser optionParser,
      final OptionSet options,
      final Set<OptionSpec<?>> allOptions,
      final OptionSpec<T> option) {
    final Set<OptionSpec<?>> invalidOptions = new HashSet<>(allOptions);
    invalidOptions.remove(option);
    CommandLineUtils.checkInvalidArgs(
        optionParser,
        options,
        option,
        JavaConverters.asScalaSetConverter(invalidOptions).asScala());
  }

  protected void parseArguments(final String[] args) {
    final OptionParser optionParser = new OptionParser(false);
    applicationIdOption =
        optionParser
            .accepts("application-id", "The Kafka Streams application ID (application.id).")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("id")
            .required();
    bootstrapServerOption =
        optionParser
            .accepts(
                "bootstrap-servers",
                "Comma-separated list of broker urls with format: HOST1:PORT1,HOST2:PORT2")
            .withRequiredArg()
            .ofType(String.class)
            .defaultsTo("localhost:9092")
            .describedAs("urls");
    instanceIdOption =
        optionParser
            .accepts(
                "instance-ids",
                "Comma-separated list of instance ids. The tool will remove this instances from the list of members of the consumer group.")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',')
            .describedAs("list");
    prefixKeepOption =
        optionParser
            .accepts("prefix-keep", "Remove all instances whose id doesn't match this prefix")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("prefix");
    commandConfigOption =
        optionParser
            .accepts(
                "config-file",
                "Property file containing configs to be passed to admin clients and embedded consumer.")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("file name");
    executeOption =
        optionParser
            .accepts(
                "execute",
                "Set this flag to true to execute remove command. Otherwise just will inform you of the result.")
            .withRequiredArg()
            .ofType(Boolean.class)
            .defaultsTo(false)
            .describedAs("execute");

    helpOption = optionParser.accepts("help", "Print usage information.").forHelp();
    versionOption =
        optionParser.accepts("version", "Print version information and exit.").forHelp();

    try {
      options = optionParser.parse(args);

      if (args.length == 0 || options.has(helpOption)) {
        CommandLineUtils.printUsageAndDie(optionParser, USAGE);
      }

      if (options.has(versionOption)) {
        CommandLineUtils.printVersionAndDie();
      }
    } catch (final OptionException e) {
      CommandLineUtils.printUsageAndDie(optionParser, e.getMessage());
    }

    final Set<OptionSpec<?>> allScenarioOptions = new HashSet<>();
    allScenarioOptions.add(instanceIdOption);
    allScenarioOptions.add(prefixKeepOption);

    checkInvalidArgs(optionParser, options, allScenarioOptions, instanceIdOption);
    checkInvalidArgs(optionParser, options, allScenarioOptions, prefixKeepOption);
  }

  public static void main(final String[] args) {
    Exit.exit(new RemoveGroupMembersTool().run(args));
  }
}
