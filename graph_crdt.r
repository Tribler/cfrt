#!/usr/bin/Rscript --verbose

library(dplyr)
library(ggplot2)

# step 1, read data
expdata <- data.frame(Run=factor(), Replica=factor(), timestamp=double(), crdttype=factor(), entry_count=double(), entry_size=double(), merge_count=double(), merge_time=double(), unpickle_time=double(), buffer_fragment_time=double(), broadcast_time=double())
for (datafile in list.files(pattern="crdt.csv")){
    df <- read.csv(datafile, header=FALSE, sep=";", col.names = c("Run", "Replica", "timestamp", "crdttype", "entry_count", "entry_size", "merge_count", "merge_time", "unpickle_time", "buffer_fragment_time", "broadcast_time"))
    if (nrow(df)==0)
        next
    expdata <- rbind(expdata, df)
}

# step 2, normalize data
expdata$Replica <- as.factor(expdata$Replica)
expdata$crdttype <- as.factor(expdata$crdttype)
for (crdt_type in levels(expdata$crdttype)) {
    min_time <- min(expdata[expdata$crdttype == crdt_type, "timestamp"])
    print(paste("Min time for", crdt_type, "is", min_time))
    expdata[expdata$crdttype == crdt_type, "timestamp"] <- expdata[expdata$crdttype == crdt_type, "timestamp"] - min_time
}

expdata <- summarise(group_by(expdata, Run, timestamp, crdttype), mean_count = mean(entry_count), mean_size = mean(entry_size), mean_merge_count = mean(merge_count), mean_merge_time=mean(merge_time), mean_unpickle_time=mean(unpickle_time), mean_buffer_fragment_time=mean(buffer_fragment_time), mean_broadcast_time=mean(broadcast_time))

# step 3, plot data

for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ], aes(timestamp, mean_size/1024, group=crdttype, colour=crdttype)) + geom_line(size=1.15) + xlim(0, max(expdata$timestamp)) + ylim(min(expdata$mean_size)/1024, max(expdata$mean_size)/1024)
    p <- p + labs(x = "Time into experiment (Seconds)", y = "Average message size (KBytes)", colour = "Replica Type") + scale_color_discrete(limits=c("CrdtSet", "NaiveORSet", "OptORSet"), labels = c("BloomCRDT", "OR-Set", "OptOR-Set"))
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("entry_size_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("entry_size_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}

for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ], aes(timestamp, mean_count, group=crdttype, colour=crdttype)) + geom_line(size=1.15) + xlim(0, max(expdata$timestamp)) + ylim(min(expdata$mean_count), max(expdata$mean_count))
    p <- p + labs(x = "Time into experiment (Seconds)", y = "Average element count", colour = "Replica Type") + scale_color_discrete(limits=c("CrdtSet", "NaiveORSet", "OptORSet"), labels = c("BloomCRDT", "OR-Set", "OptOR-Set"))
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("entry_count_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("entry_count_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}

for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ], aes(timestamp, mean_merge_count, group=crdttype, colour=crdttype)) + geom_line(size=1.15) + xlim(0, max(expdata$timestamp)) + ylim(min(expdata$mean_merge_count), max(expdata$mean_merge_count))
    p <- p + labs(x = "Time into experiment (Seconds)", y = "Average merge count", colour = "Replica Type") + scale_color_discrete(limits=c("CrdtSet", "NaiveORSet", "OptORSet"), labels = c("BloomCRDT", "OR-Set", "OptOR-Set"))
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("merge_count_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("merge_count_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}

for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ], aes(timestamp, mean_merge_time, group=crdttype, colour=crdttype)) + geom_line(size=1.15) + xlim(0, max(expdata$timestamp)) + ylim(min(expdata$mean_merge_time), max(expdata$mean_merge_time))
    p <- p + labs(x = "Time into experiment (Seconds)", y = "Average merge time (Seconds)", colour = "Replica Type") + scale_color_discrete(limits=c("CrdtSet", "NaiveORSet", "OptORSet"), labels = c("BloomCRDT", "OR-Set", "OptOR-Set"))
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("merge_time_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("merge_time_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}

for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ], aes(timestamp, mean_unpickle_time, group=crdttype, colour=crdttype)) + geom_line(size=1.15) + xlim(0, max(expdata$timestamp)) + ylim(min(expdata$mean_unpickle_time), max(expdata$mean_unpickle_time))
    p <- p + labs(x = "Time into experiment (Seconds)", y = "Average unpickle time (Seconds)", colour = "Replica Type") + scale_color_discrete(limits=c("CrdtSet", "NaiveORSet", "OptORSet"), labels = c("BloomCRDT", "OR-Set", "OptOR-Set"))
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("mean_unpickle_time_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("mean_unpickle_time_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}

for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ], aes(timestamp, mean_unpickle_time+mean_merge_time+mean_buffer_fragment_time, group=crdttype, colour=crdttype)) + geom_line(size=1.15) + xlim(0, max(expdata$timestamp)) + ylim(min(expdata$mean_unpickle_time+expdata$mean_merge_time+expdata$mean_buffer_fragment_time), max(expdata$mean_unpickle_time+expdata$mean_merge_time+expdata$mean_buffer_fragment_time))
    p <- p + labs(x = "Time into experiment (Seconds)", y = "Average message processing time (Seconds)", colour = "Replica Type") + scale_color_discrete(limits=c("CrdtSet", "NaiveORSet", "OptORSet"), labels = c("BloomCRDT", "OR-Set", "OptOR-Set"))
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("mean_time_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("mean_unpickle_time_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}

for (run_count in levels(as.factor(expdata$Run))) {
    p <- ggplot(data=expdata[expdata$Run == run_count, ], aes(timestamp, mean_broadcast_time, colour=crdttype)) + geom_line(size=1.15) + xlim(0, max(expdata$timestamp)) + ylim(min(expdata$mean_broadcast_time), max(expdata$mean_broadcast_time))
    p <- p + labs(x = "Time into experiment (Seconds)", y = "Average broadcast time (Seconds)") + scale_color_discrete(name="Replica Type", limits=c("CrdtSet", "NaiveORSet", "OptORSet"), labels = c("BloomCRDT", "OR-Set", "OptOR-Set"))
    p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
    tryCatch(ggsave(file=paste("mean_broadcast_time_run_", run_count, ".svg", sep=""), plot=p, width=8, height=5, dpi=300), error = function(e) { ggsave(file=paste("mean_broadcast_time_run_", run_count, ".png", sep=""), plot=p, width=8, height=5, dpi=600) })
}