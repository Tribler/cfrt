#!/usr/bin/Rscript --verbose

library(dplyr)
library(ggplot2)

# step 1, read data
expdata <- data.frame(Run=factor(), Replica=factor(), timestamp=double(), root_size=factor(), entry_count=double(), add_count=double(), remove_count=double(), node_count=double(), touch_count=double(), touch_size=double(), check_count=double(), check_size=double(), cfrt_split_count=double(), cfrt_merge_count=double(), merge_count=double(), pass_count=double(), drop_count=double(), merge_time=double(), unpickle_time=double(), buffer_fragment_time=double(), broadcast_time=double())
for (datafile in list.files(pattern="cfrt.csv")){
    df <- read.csv(datafile, header=FALSE, sep=";", col.names = c("Run", "Replica", "timestamp", "root_size", "entry_count", "add_count", "remove_count", "node_count", "touch_count", "touch_size", "check_count", "check_size", "cfrt_split_count", "cfrt_merge_count", "merge_count", "pass_count", "drop_count", "merge_time", "unpickle_time", "buffer_fragment_time", "broadcast_time"))
    if (nrow(df)==0)
        next
    # df$Replica <- sub("download-progress-(\\d+).csv", "Peer \\1", datafile)
    expdata <- rbind(expdata, df)
}

# step 2, normalize data
expdata$Replica <- as.factor(expdata$Replica)

min_time <- min(expdata$timestamp)
print(paste("Min time is", min_time))
expdata$timestamp <- expdata$timestamp - min_time


for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ], aes(timestamp, entry_count, colour=Replica)) + geom_line(size=1) + xlim(0, max(expdata$timestamp))
    p <- p + labs(x = "Time into experiment (Seconds)", y = "Number of CFRT entries")
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("exact_entry_count_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("exact_entry_count_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}

expdata <- summarise(group_by(expdata, Run, timestamp, root_size), mean_entry_count = mean(entry_count), mean_add_count = mean(add_count), mean_remove_count = mean(remove_count), mean_node_count=mean(node_count), mean_touch_count=mean(touch_count), mean_touch_size=mean(touch_size)/1024, mean_check_count=mean(check_count), mean_check_size=mean(check_size)/1024, mean_cfrt_split_count = mean(cfrt_split_count), mean_cfrt_merge_count = mean(cfrt_merge_count), mean_merge_count = mean(merge_count), mean_pass_count=mean(pass_count), mean_drop_count=mean(drop_count), mean_merge_time=mean(merge_time), mean_unpickle_time=mean(unpickle_time), mean_buffer_fragment_time=mean(buffer_fragment_time), mean_broadcast_time=mean(broadcast_time))

# step 3, plot data
for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ]) + geom_line(size=1, aes(timestamp, mean_pass_count, colour="Passed")) + geom_line(size=1, aes(timestamp, mean_drop_count, colour="Dropped")) + geom_line(size=1, aes(timestamp, mean_pass_count+mean_drop_count, colour="Passed+Dropped")) + xlim(0, max(expdata$timestamp)) + ylim(0, 2750)
    p <- p + labs(x = "Time into experiment (Seconds)", y = "Number of messages", colour = "Action")
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("monkey_count_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("monkey_count_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}

for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ], aes(timestamp, mean_add_count, colour=root_size)) + geom_line(size=1) + xlim(0, max(expdata$timestamp)) + ylim(min(expdata$mean_add_count), max(expdata$mean_add_count))
    p <- p + labs(x = "Time into experiment (Seconds)", y = "Number of keys added", colour = "Type") + scale_color_discrete(limits=c("finite", "infinite"), labels = c("CFRT", "BloomCRDT"))
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("added_count_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("added_count_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}

for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ]) + geom_line(size=1, aes(timestamp, mean_node_count, colour="Create", linetype=root_size)) + geom_line(size=1, aes(timestamp, mean_touch_count, colour="Fetch", linetype=root_size)) + geom_line(size=1, aes(timestamp, mean_check_count, colour="Check", linetype=root_size)) + xlim(0, max(expdata$timestamp))
    p <- p + scale_y_log10("Number of CFRT Tree Nodes, log()", limits = c(1, max(expdata$mean_node_count, expdata$mean_touch_count, expdata$mean_check_count)))
    p <- p + labs(x = "Time into experiment (Seconds)", colour = "Operation", linetype="Type") + scale_linetype_discrete(limits=c("finite", "infinite"), labels = c("CFRT", "BloomCRDT"))
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("node_count_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("node_count_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}

for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ]) + geom_line(size=1, aes(timestamp, mean_touch_size, colour="Fetch", linetype=root_size)) + geom_line(size=1, aes(timestamp, mean_check_size, colour="Check", linetype=root_size)) + xlim(0, max(expdata$timestamp))  # + ylim(min(expdata$mean_touch_size, expdata$mean_check_size), max(expdata$mean_touch_count, expdata$mean_check_count))
    p <- p + labs(x = "Time into experiment (Seconds)", y = "Transmission size (KBytes)", colour = "Operation", linetype="Type") + scale_linetype_discrete(limits=c("finite", "infinite"), labels = c("CFRT", "BloomCRDT"))
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("sizes_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("sizes_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}
