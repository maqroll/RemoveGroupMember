package streams.tool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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

/**
 */
public class RemoveGroupMembersTool {
    private static final int EXIT_CODE_SUCCESS = 0;
    private static final int EXIT_CODE_ERROR = 1;

    private static OptionSpec<String> bootstrapServerOption;
    private static OptionSpec<String> applicationIdOption;
    private static OptionSpec<String> instanceIdOption;
    private static OptionSpec<Void> helpOption;
    private static OptionSpec<Void> versionOption;
    private static OptionSpec<String> commandConfigOption;

    private final static String USAGE = "";

    private OptionSet options = null;
    private final List<String> allTopics = new LinkedList<>();


    public int run(final String[] args) {
        return run(args, new Properties());
    }

    public int run(final String[] args,
        final Properties config) {
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
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServerOption));

            adminClient = Admin.create(properties);
            exitCode = deleteActiveConsumers(groupId, adminClient, instanceIds);
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

    protected int deleteActiveConsumers(final String groupId,
        final Admin adminClient, final List<String> instanceIds)
        throws ExecutionException, InterruptedException {

        final DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(
            Collections.singleton(groupId),
            new DescribeConsumerGroupsOptions().timeoutMs(10 * 1000));
        final List<MemberDescription> members =
            new ArrayList<>(describeResult.describedGroups().get(groupId).get().members());
        final List<String> staticMembers = members.stream().filter(memberDescription -> memberDescription.groupInstanceId().isPresent()).map(memberDescription -> memberDescription.groupInstanceId().get()).collect(
            Collectors.toList());
        final List<String> instancesPresent = staticMembers.stream().filter(id -> instanceIds
            .contains(id)).collect(Collectors.toList());
        final List<String> missingInstanceIds =
            instanceIds.stream().filter(instanceIdToRemove -> !instancesPresent.contains(instanceIdToRemove)).collect(Collectors.toList());
        if (missingInstanceIds.isEmpty()) {
            Set<MemberToRemove> membersToRemove = instanceIds.stream().map(s -> new MemberToRemove(s)).collect(Collectors.toSet());
            adminClient.removeMembersFromConsumerGroup(groupId, new RemoveMembersFromConsumerGroupOptions(membersToRemove)).all().get();
        } else {
            throw new IllegalStateException("Refuse to remove following members: " + missingInstanceIds + " from application " + groupId
             + " because they are no active. Active members: " + staticMembers);
        }
        return 0;
    }

    private void parseArguments(final String[] args) {
        final OptionParser optionParser = new OptionParser(false);
        applicationIdOption = optionParser.accepts("application-id", "The Kafka Streams application ID (application.id).")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("id")
            .required();
        bootstrapServerOption = optionParser.accepts("bootstrap-servers", "Comma-separated list of broker urls with format: HOST1:PORT1,HOST2:PORT2")
            .withRequiredArg()
            .ofType(String.class)
            .defaultsTo("localhost:9092")
            .describedAs("urls");
        instanceIdOption = optionParser.accepts("instance-ids", "Comma-separated list of instance ids. The tool will remove this instances from the list of members of the consumer group.")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',')
            .describedAs("list")
            .required();
        commandConfigOption = optionParser.accepts("config-file", "Property file containing configs to be passed to admin clients and embedded consumer.")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("file name");

        helpOption = optionParser.accepts("help", "Print usage information.").forHelp();
        versionOption = optionParser.accepts("version", "Print version information and exit.").forHelp();

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
    }

    public static void main(final String[] args) {
        Exit.exit(new RemoveGroupMembersTool().run(args));
    }

}