#!/usr/bin/Rscript --verbose

library(dplyr)
library(ggplot2)

# step 1, read data
expdata <- data.frame(software=factor(), element=double(), time_taken=double())

df <- read.csv('tribler_times_numbered.csv', header=FALSE, sep=";", col.names = c('element', 'time_taken', 'validation_tag'))
df$software <- "tribler"
expdata <- rbind(expdata, df)

df <- read.csv('cfrt_times_numbered.csv', header=FALSE, sep=";", col.names = c('element', 'time_taken', 'validation_tag'))
df$software <- "cfrt"
expdata <- rbind(expdata, df)

log_breaks = function(maj, radix=10) {
  function(x) {
    minx         = floor(min(logb(x,radix), na.rm=T)) - 1
    maxx         = ceiling(max(logb(x,radix), na.rm=T)) + 1
    n_major      = maxx - minx + 1
    major_breaks = seq(minx, maxx, by=1)
    if (maj) {
      breaks = major_breaks
    } else {
      steps = logb(1:(radix-1),radix)
      breaks = rep(steps, times=n_major) +
               rep(major_breaks, each=radix-1)
    }
    radix^breaks
  }
}
scale_y_log_eng = function(..., radix=10) {
  scale_y_log10(...,
                     breaks=log_breaks(TRUE, radix),
                     minor_breaks=log_breaks(FALSE, radix))
}

p <- ggplot(data=expdata, aes(x=element, y=time_taken, colour=software)) + geom_line(size=1) + scale_y_log_eng(labels = function(x) format(x, big.mark = ",", scientific = FALSE)) + scale_x_continuous(labels=function(x) format(x, big.mark=",", decimal.mark = ".", scientific = FALSE))
p <- p + labs(x = "Torrent", y = "Time to access (seconds)", colour="Method") + scale_color_discrete(limits=c("tribler", "cfrt"), labels = c("Tribler", "CFRT"))
p <- p + theme_bw() + theme(legend.position = "bottom", text = element_text(size = 16)) #+ theme(legend.box.background = element_rect(colour = "black"))
p <- p + theme(axis.text.x = element_text(angle=45, hjust = 1))
tryCatch(ggsave(file="access_times.svg", plot=p, width=8, height=5, dpi=300), error=function (e) { })
ggsave(file="access_times.png", plot=p, width=8, height=5, dpi=600)

