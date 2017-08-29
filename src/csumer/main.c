#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>
#include <getopt.h>

#include <librdkafka/rdkafka.h>
#include <avro.h>


static char *brokers = "kafka:9092";
static char *group = "consumers";
static char *client = "csumer";
static char *topic = "foobar";
static int partition = 2;
static char *debug = NULL;

avro_schema_t value_schema;
avro_schema_t key_schema;
static char *value_schema_path = "/value_schema.avsc";
static char *key_schema_path = "/key_schema.avsc";
int64_t id = 0;

static int run = 1;
static rd_kafka_t *rk;
static int exit_eof = 0;
static int wait_eof = 0;
static int quiet = 1;

static enum {
    OUTPUT_RAW,
    OUTPUT_TEXT,
} output = OUTPUT_TEXT;

static void stop (int sig) {
    if (!run)
        exit(1);
    run = 0;
    fclose(stdin);
}

// Read avro schema
void init_schema (char *schema_path, avro_schema_t *schema)
{
    FILE *fp;
    long len;
    char *buffer;

    fp = fopen (schema_path , "rb");

    if(!fp) {
        perror(schema_path);
        exit(1);
    }

    fseek(fp , 0L , SEEK_END);
    len = ftell(fp);
    fseek (fp, 0, SEEK_SET);

    buffer = malloc(len + 1);
    if(!buffer) {
        fclose(fp);
        fputs("memory alloc fails", stderr);
        exit(1);
    }
    if (1 != fread(buffer, len, 1, fp)) {
        fclose(fp);
        free(buffer);
        fputs("entire read fails", stderr);
        exit(1);
    }
    fclose(fp);
    buffer[len] = '\0';

    avro_schema_error_t error;
    avro_schema_from_json(buffer, len, schema, &error);

    if (error) {
        fprintf(stderr, "Unable to parse schema: %s\n", avro_strerror());
        exit(EXIT_FAILURE);
    }
    free(buffer);
}


static void keydump (FILE *fp, const void *ptr, size_t len) {
    char *p;
    avro_datum_t key;
    avro_reader_t reader = avro_reader_memory(ptr, len);
    avro_read_data(reader, key_schema, key_schema, &key);
    avro_string_get(key, &p);
    fprintf(fp, "%-6s - ", p);
}

static void avrodump (FILE *fp, const void *ptr, size_t len) {
    const char *p = (const char *)ptr;
    avro_reader_t reader = avro_reader_memory(p, len);

    int rval;
    avro_datum_t ad;

    rval = avro_read_data(reader, value_schema, value_schema, &ad);
    if (rval == 0) {
        int32_t i32;
        char *p;
        avro_datum_t id_datum, subject_datum, price_datum;

        if (avro_record_get(ad, "id", &id_datum) == 0) {
            avro_int32_get(id_datum, &i32);
            fprintf(fp, "id: %-4d ", i32);
        }
        if (avro_record_get(ad, "subject", &subject_datum) == 0) {
            avro_string_get(subject_datum, &p);
            fprintf(fp, "subject: %-7s ", p);
        }
        if (avro_record_get(ad, "price", &price_datum) == 0) {
            avro_int32_get(price_datum, &i32);
            fprintf(fp, "price: %-6d ", i32);
        }
        fprintf(fp, "\n");

        avro_datum_decref(id_datum);
        avro_datum_decref(subject_datum);
        avro_datum_decref(price_datum);
    }
}

static void logger (const rd_kafka_t *rk, int level,
        const char *fac, const char *buf) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    fprintf(stdout, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
            (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
            level, fac, rd_kafka_name(rk), buf);
}

static void msg_consume (rd_kafka_message_t *rkmessage,
        void *opaque) {
    if (rkmessage->err) {
        if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            if (quiet == 0) {
                fprintf(stderr,
                    "%% Consumer reached end of %s [%"PRId32"] "
                    "message queue at offset %"PRId64"\n",
                    rd_kafka_topic_name(rkmessage->rkt),
                    rkmessage->partition, rkmessage->offset);
            }

            if (exit_eof && --wait_eof == 0) {
                fprintf(stderr,
                        "%% All partition(s) reached EOF: "
                        "exiting\n");
                run = 0;
            }

            return;
        }

        if (rkmessage->rkt)
            fprintf(stderr, "%% Consume error for "
                    "topic \"%s\" [%"PRId32"] "
                    "offset %"PRId64": %s\n",
                    rd_kafka_topic_name(rkmessage->rkt),
                    rkmessage->partition,
                    rkmessage->offset,
                    rd_kafka_message_errstr(rkmessage));
        else
            fprintf(stderr, "%% Consumer error: %s: %s\n",
                    rd_kafka_err2str(rkmessage->err),
                    rd_kafka_message_errstr(rkmessage));

        if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
                rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
            run = 0;
        return;
    }

    if (!quiet) {
        fprintf(stdout, "%% Message (topic %s [%"PRId32"], "
                "offset %"PRId64", %zd bytes):\n",
                rd_kafka_topic_name(rkmessage->rkt),
                rkmessage->partition,
                rkmessage->offset, rkmessage->len);
    }

    fprintf(stderr,
        "%s[%"PRId32"]@%-4"PRId64" - ",
        rd_kafka_topic_name(rkmessage->rkt),
        rkmessage->partition,
        rkmessage->offset);

    if (rkmessage->key_len) {
        switch (output) {
            case OUTPUT_TEXT:
                keydump(stderr, rkmessage->key, rkmessage->key_len);
                break;

            default:
                printf("Key: %.*s\n", (int)rkmessage->key_len, (char *)rkmessage->key);
                break;
        }
    }

    switch (output) {
        case OUTPUT_TEXT:
            avrodump(stderr, rkmessage->payload, rkmessage->len);
            break;

        default:
            printf("%.*s\n", (int)rkmessage->len, (char *)rkmessage->payload);
            break;
    }
}


static void print_partition_list (FILE *fp,
        const rd_kafka_topic_partition_list_t
        *partitions) {
    int i;
    for (i = 0 ; i < partitions->cnt ; i++) {
        fprintf(stderr, "%s %s [%"PRId32"] offset %"PRId64,
                i > 0 ? ",":"",
                partitions->elems[i].topic,
                partitions->elems[i].partition,
                partitions->elems[i].offset);
    }
    fprintf(stderr, "\n");

}

static void rebalance_cb (rd_kafka_t *rk,
        rd_kafka_resp_err_t err,
        rd_kafka_topic_partition_list_t *partitions,
        void *opaque) {

    fprintf(stderr, "%% Consumer group rebalanced: ");

    switch (err)
    {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            fprintf(stderr, "assigned:\n");
            print_partition_list(stderr, partitions);
            rd_kafka_assign(rk, partitions);
            wait_eof += partitions->cnt;
            break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            fprintf(stderr, "revoked:\n");
            print_partition_list(stderr, partitions);
            rd_kafka_assign(rk, NULL);
            wait_eof = 0;
            break;

        default:
            fprintf(stderr, "failed: %s\n",
                    rd_kafka_err2str(err));
            rd_kafka_assign(rk, NULL);
            break;
    }
}

static void sig_usr1 (int sig) {
    rd_kafka_dump(stdout, rk);
}

int main (int argc, char **argv) {
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topic_conf;
    char errstr[512];
    char tmp[16];
    rd_kafka_resp_err_t err;
    rd_kafka_topic_partition_list_t *topics;

    quiet = !isatty(STDIN_FILENO);

    conf = rd_kafka_conf_new();

    rd_kafka_conf_set_log_cb(conf, logger);

    snprintf(tmp, sizeof(tmp), "%i", SIGIO);
    rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);

    signal(SIGINT, stop);
    signal(SIGUSR1, sig_usr1);

    if (output == OUTPUT_TEXT) {
        init_schema(value_schema_path, &value_schema);
        init_schema(key_schema_path, &key_schema);
    }

    if (debug &&
            rd_kafka_conf_set(conf, "debug", debug, errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% Debug configuration failed: %s: %s\n",
                errstr, debug);
        exit(1);
    }

    // Add to group
    if (rd_kafka_conf_set(conf, "group.id", group,
                errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", errstr);
        exit(1);
    }

    if (rd_kafka_conf_set(conf, "client.id", client,
                errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", errstr);
        exit(1);
    }

    topic_conf = rd_kafka_topic_conf_new();

    if (rd_kafka_topic_conf_set(topic_conf, "offset.store.method",
                "broker",
                errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", errstr);
        exit(1);
    }

    if (rd_kafka_topic_conf_set(topic_conf, "auto.offset.reset",
                "earliest",
                errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", errstr);
        exit(1);
    }

    rd_kafka_conf_set_default_topic_conf(conf, topic_conf);
    rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

    if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
                    errstr, sizeof(errstr)))) {
        fprintf(stderr,
                "%% Failed to create new consumer: %s\n",
                errstr);
        exit(1);
    }

    // Add brokers
    if (rd_kafka_brokers_add(rk, brokers) == 0) {
        fprintf(stderr, "%% No valid brokers specified\n");
        exit(1);
    }

    rd_kafka_poll_set_consumer(rk);

    // Subscribe to topic
    topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic, partition);

    fprintf(stderr, "%% Subscribing to %d topics\n", topics->cnt);

    if ((err = rd_kafka_subscribe(rk, topics))) {
        fprintf(stderr,
                "%% Failed to start consuming topics: %s\n",
                rd_kafka_err2str(err));
        exit(1);
    }

    while (run) {
        rd_kafka_message_t *rkmessage;

        rkmessage = rd_kafka_consumer_poll(rk, 1000);
        if (rkmessage) {
            msg_consume(rkmessage, NULL);
            rd_kafka_message_destroy(rkmessage);
        }
    }

    err = rd_kafka_consumer_close(rk);
    if (err)
            fprintf(stderr, "%% Failed to close consumer: %s\n",
                    rd_kafka_err2str(err));
    else
            fprintf(stderr, "%% Consumer closed\n");

    rd_kafka_topic_partition_list_destroy(topics);
    rd_kafka_destroy(rk);

    run = 5;
    while (run-- > 0 && rd_kafka_wait_destroyed(1000) == -1)
        printf("Waiting for librdkafka to decommission\n");
    if (run <= 0)
        rd_kafka_dump(stdout, rk);

    return 0;
}
