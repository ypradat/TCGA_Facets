# created: Oct 03 2022
# modified: Oct 05 2022
# author: Yoann Pradat
# 
#     CentraleSupelec
#     MICS laboratory
#     9 rue Juliot Curie, Gif-Sur-Yvette, 91190 France
# 
#     Institut Gustave Roussy
#     Prism Center
#     114 rue Edouard Vaillant, Villejuif, 94800 France
# 
# Compute chromosome arm copy-number changes using CN segments estimated by FACETS.

suppressPackageStartupMessages(library(argparse))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(facetsSuite))

# functions ============================================================================================================

read_header <- function(path, prefix){
  hea <- list()
  if (grepl(".gz$", path)){
    con <- gzfile(path, "rt")
  } else {
    con <- file(path, "r")
  }
  i <- 1
  while ( TRUE ) {
    line <- readLines(con, n = 1)
    if ( length(line) == 0 | !grepl(paste0("^", prefix), line)) {
      break
    } else {
      hea[[i]] <- line
      i <- i+1
    }
  }
  close(con)

  hea
}


load_table <- function(path, header_prefix=NULL, ...){
  if (is.null(path)) return(NULL)
  args <- list(...)

  if (!"delim" %in% names(args)){
    if (grepl(".csv", path)){
      args$delim <- ","
    } else if (grepl(".tsv", path) | grepl(".txt", path)){
      args$delim <- "\t"
    }
  }

  if (!is.null(header_prefix)){
    header <- read_header(path, prefix=header_prefix)
    args$skip <- length(header) 
  }

  if (!"progress" %in% names(args)){
    args$progress <- F
  }
  if (!"show_col_types" %in% names(args)){
    args$show_col_types <- F
  }

  if(grepl(".gz$", path)) {
    file <- base::gzfile(path)
    df <- do.call(readr::read_delim, c(list(file=file), args))
  } else {
    df <- do.call(readr::read_delim, c(path, args))
  }

  df
}


extract_from_header <- function(header, field, prefix="##", suffix=NULL){
  regex <- paste0("^", prefix, field, "=")
  line <- header[grepl(regex, header)] 

  if (identical(line, character(0))){
    warning(paste("the pattern", regex, "was not found in the header!"))
    value <- NULL
  } else if (length(line)>1){
    warning(paste("the pattern", regex, "was found more than once!"))
    value <- NULL
  } else {
    value <- gsub(regex, "", line)
    if (!is.null(suffix)){
      value <- gsub(paste0(suffix, "$",  "", line))
    }
    value <- as.numeric(value)
  }

  value
}


extract_genome_sizes <- function(header){
  contigs <- header[grepl("^##contig", header)]
  chroms <- unlist(lapply(contigs, function(contig) gsub(",.*$", "", gsub("##contig=<ID=", "", contig))))
  sizes <- unlist(lapply(contigs, function(contig) gsub(">$", "", gsub("^.*length=", "", contig))))
  genome <- data.frame(chrom=chroms, size=sizes)

  genome$size <- as.numeric(genome$size)
  mask_char <- genome$chrom %in% as.character(1:22)
  genome[mask_char, "chrom"] <- as.numeric(genome[mask_char, "chrom"])

  genome
}

# reimplementation of facetsSuite:::join_segments because of mysterious memory usage bug in the chunk of code
#
#   n_3mb = which((q_arm$end-q_arm$start) < 3e6)
#   while (length(n_3mb) > 0) {
#       q_arm = q_arm[-(n_3mb[1]), ] # non-juxtaposed segments will be removed, which is fine
#       q_arm = join_segments(q_arm)
#       n_3mb = which((q_arm$end - q_arm$start) < 3e6)
#   }
#

# Join segments with identical copy number that are separated for some reason
# Logic: if different tcn ==> don't join
# if any NAs in mcn/lcn ==> don't join
# if identical mcn/lcn content ==> do join
join_segments = function(chrom_seg) {
  # if (nrow(chrom_seg) < 2) {
  #   chrom_seg
  # } else {
  #   new_chr = chrom_seg
  #   seg_class = c(1)
  #   for (j in 2:nrow(new_chr)) {
  #     # if adjacent segments have same allelic content, assign to same class
  #     if (new_chr[(j - 1), 'tcn'] != new_chr[j, 'tcn']) {
  #       # if tcn differs, definitely don't condense
  #       seg_class = c(seg_class, seg_class[j - 1] + 1)
  #     } else if (any(is.na(c(new_chr[(j - 1), 'mcn'], new_chr[(j), 'mcn'])))) {
  #       # but if tcn the same, check for difference in allelic content
  #       seg_class = c(seg_class, seg_class[j - 1] + 1)
  #     } else if (new_chr[(j - 1), 'mcn'] == new_chr[j, 'mcn'] &
  #                new_chr[(j - 1), 'lcn'] == new_chr[j, 'lcn']) {
  #       seg_class = c(seg_class, seg_class[j - 1])
  #     } else {
  #       seg_class = c(seg_class, seg_class[j - 1] + 1)
  #     }
  #   }
  #   for (j in unique(seg_class)) {
  #     # condense segments belonging to same class
  #     new_chr[seg_class %in% j, 'end'] = max(new_chr[seg_class %in% j, 'end'])
  #     new_chr[seg_class %in% j, 'num.mark'] = sum(new_chr[seg_class %in% j, 'num.mark'])
  #   }
  #   new_chr[!duplicated(seg_class), ]
  # }
  if (nrow(chrom_seg) < 2){
    chrom_seg_new <- chrom_seg
  } else {
    seg_class = c(1)
    for (j in 2:nrow(chrom_seg)) {
      # if adjacent segments have same allelic content, assign to same class
      if (chrom_seg[(j - 1), 'tcn'] != chrom_seg[j, 'tcn']) {
        # if tcn differs, definitely don't condense
        seg_class = c(seg_class, seg_class[j - 1] + 1)
      } else if (any(is.na(c(chrom_seg[(j - 1), 'mcn'], chrom_seg[(j), 'mcn'])))) {
        # but if tcn the same, check for difference in allelic content
        seg_class = c(seg_class, seg_class[j - 1] + 1)
      } else if (chrom_seg[(j - 1), 'mcn'] == chrom_seg[j, 'mcn'] &
                 chrom_seg[(j - 1), 'lcn'] == chrom_seg[j, 'lcn']) {
        seg_class = c(seg_class, seg_class[j - 1])
      } else {
        seg_class = c(seg_class, seg_class[j - 1] + 1)
      }
    }

    # condense segments belonging to same class
    chrom_seg_news <- list()
    i <- 1
    for (j in unique(seg_class)){
      chrom_seg_new_j <- chrom_seg[i,]
      chrom_seg_new_j["end"] <- max(chrom_seg[seg_class==j, "end"])
      chrom_seg_new_j["num.mark"] <- sum(chrom_seg[seg_class==j, "num.mark"])
      i <- i + sum(seg_class==j)
      chrom_seg_news <- c(chrom_seg_news, list(chrom_seg_new_j))
    }
    chrom_seg_new <- bind_rows(chrom_seg_news)
  }

  chrom_seg_new
}

# reimplementation of facetsSuite::calculate_lst because of mysterious memory usage bug in the chunk of code
#
#   n_3mb = which((q_arm$end-q_arm$start) < 3e6)
#   while (length(n_3mb) > 0) {
#       q_arm = q_arm[-(n_3mb[1]), ] # non-juxtaposed segments will be removed, which is fine
#       q_arm = join_segments(q_arm)
#       n_3mb = which((q_arm$end - q_arm$start) < 3e6)
#   }
#

calculate_lst_custom <-  function(segs,
                                  ploidy,
                                  genome = c('hg19', 'hg18', 'hg38'),
                                  algorithm = c('em', 'cncf'),
                                  min_size = 10e6) {
    
  algorithm = match.arg(algorithm, c('em', 'cncf'), several.ok = FALSE)

  # Centromere locations
  genome = eval(parse(text=paste0("facetsSuite:::", genome)))

  # Create chrom_info for sample
  segs = facetsSuite:::parse_segs(segs, algorithm) %>% 
    filter(chrom %in% 1:22)
  sample_chrom_info = facetsSuite:::get_sample_genome(segs, genome)

  # Count LSTs
  cols_arm <- c("chrom", "seg", "start", "end", "tcn", "mcn", "lcn", "num.mark")
  lst = c()
  for (chr in unique(segs$chrom)) {
    chrom_segs = segs[segs$chrom == chr, ]
    if (nrow(chrom_segs) < 2) next

    # Split into chromosome arms
    # segments starting on p arm, these might overlap centromere
    p_arm = chrom_segs[chrom_segs$start <= sample_chrom_info$centromere[sample_chrom_info$chr == chr], ]
    # segments ending on q arm
    q_arm = chrom_segs[chrom_segs$end >= sample_chrom_info$centromere[sample_chrom_info$chr == chr], ]

    p_arm = join_segments(p_arm) # shrink segments with same CN
    q_arm = join_segments(q_arm)

    # cut p-arm segment spanning centromere at centromere start
    if (nrow(p_arm) > 0) p_arm[nrow(p_arm), 'end'] = sample_chrom_info$centromere[sample_chrom_info$chr == chr]

    # set first q arm segment to start at centromere end
    if (nrow(q_arm) > 0) q_arm[1, 'start'] = sample_chrom_info$centromere[sample_chrom_info$chr == chr]

    # P arm
    p_arm <- p_arm  %>% select(all_of(cols_arm)) %>% as_tibble()

    # Smoothen 3-Mb segments
    n_3mb = which((p_arm$end-p_arm$start) < 3e6)
    while (length(n_3mb) > 0) {
      p_arm = p_arm[-(n_3mb[1]), ] # non-juxtaposed segments will be removed, which is fine
      p_arm = join_segments(p_arm)
      n_3mb = which((p_arm$end - p_arm$start) < 3e6)
    }

    # Now check for LST
    if (nrow(p_arm) >= 2) { # if more than one segment
      # mark segments that pass length test
      p_arm = cbind(p_arm, c(0, 1)[match((p_arm$end - p_arm$start) >= min_size, c('FALSE', 'TRUE'))])
      for (k in 2:nrow(p_arm)) {
         # if two juxtaposed segments are 10 Mb and the space between them is less than 3 Mb...
        if (p_arm[k, ncol(p_arm)] == 1 & p_arm[(k - 1), ncol(p_arm)] == 1 &
            (p_arm[k, 'start'] - p_arm[(k-1), 'end']) < 3e6) {
          lst = c(lst, 1) # ...then add to LST
        }
      }
    }


    # Q arm
    q_arm <- q_arm  %>% select(all_of(cols_arm)) %>% as_tibble()

    # Smoothen 3-Mb segments
    n_3mb = which((q_arm$end-q_arm$start) < 3e6)
    while (length(n_3mb) > 0) {
      q_arm = q_arm[-(n_3mb[1]), ] # non-juxtaposed segments will be removed, which is fine
      q_arm = join_segments(q_arm)
      n_3mb = which((q_arm$end - q_arm$start) < 3e6)
    }

    # Now check for LST
    if (nrow(q_arm) >= 2) { # if more than one segment
      # mark segments that pass length test
      q_arm = cbind(q_arm, c(0, 1)[match((q_arm$end - q_arm$start) >= min_size, c('FALSE', 'TRUE'))])
      for (k in 2:nrow(q_arm)) {
         # if two juxtaposed segments are 10 Mb and the space between them is less than 3 Mb...
        if (q_arm[k, ncol(q_arm)] == 1 & q_arm[(k - 1), ncol(q_arm)] == 1 &
            (q_arm[k, 'start'] - q_arm[(k-1), 'end']) < 3e6) {
          lst = c(lst, 1) # ...then add to LST
        }
      }
    }
  }

  # Return values
  list(lst = sum(lst))
}

calculate_genome_fractions <- function(df_cnv_tab, genome){
  # filter to consider only autosomes
  df_cnv_tab <- df_cnv_tab %>% filter(chrom %in% 1:22)
  genome <- genome %>% filter(chrom %in% 1:22)
  genome_length <- sum(genome$size)

  # total fraction of the genome covered by losses of any level
  cnm_loss <- c("hom_del", "LOH", "cnLOH")
  cnm_gain <- c("LL_gain", "ML_gain", "HL_gain")
  loss_total <- sum(df_cnv_tab %>% filter(copy_number_more %in% cnm_loss) %>% pull(svlen)) / genome_length
  gain_total <- sum(df_cnv_tab %>% filter(copy_number_more %in% cnm_gain) %>% pull(svlen)) / genome_length

  # total fraction of the genome covered by different levels of losses and gains
  loss_del <- sum(df_cnv_tab %>% filter(copy_number_more %in% c("hom_del")) %>% pull(svlen)) / genome_length
  loss_loh_cnloh <- sum(df_cnv_tab %>% filter(copy_number_more %in% c("LOH", "cnLOH")) %>% pull(svlen)) / genome_length
  gain_hl <- sum(df_cnv_tab %>% filter(copy_number_more %in% c("HL_gain")) %>% pull(svlen)) / genome_length
  gain_ll_ml <- sum(df_cnv_tab %>% filter(copy_number_more %in% c("ML_gain", "LL_gain")) %>% pull(svlen)) / genome_length

  list(LOSS=loss_total,
       `LOSS:Deletion`=loss_del,
       `LOSS:LOH_cnLOH`=loss_loh_cnloh,
       GAIN=gain_total,
       `GAIN:HL_amplification`=gain_hl,
       `GAIN:LL_ML_amplification`=gain_ll_ml)
}


calculate_wgd <- function(df_cnv_tab, genome){
  df_cnv_tab <- df_cnv_tab %>% filter(chrom %in% 1:22) %>% mutate(mcn.em=tcn.em-lcn.em)
  genome <- genome %>% filter(chrom %in% 1:22)

  n_chr_dup <- 0
  chr_arm_dup <- data.frame()
  for (chr in 1:22){
    chr_length <- genome %>% filter(chrom==chr) %>% pull(size)
    chr_fraction <- sum(df_cnv_tab %>% filter(chrom==chr, mcn.em >= 1.5) %>% pull(svlen)) / chr_length
    if (chr_fraction >= 0.5){
      n_chr_dup <- n_chr_dup + 1
      chr_arm_dup <- bind_rows(chr_arm_dup, data.frame(chrom=chr, fraction=chr_fraction, duplicated=1))
    } else {
      chr_arm_dup <- bind_rows(chr_arm_dup, data.frame(chrom=chr, fraction=chr_fraction, duplicated=0))
    }
  }

  wgd <- 0
  if (n_chr_dup >= 11) wgd <- 1

  list(chr_arm_dup=chr_arm_dup, wgd=wgd)
}


main <- function(args){
  # load CN segments from cnv_facets
  df_cnv_tab <- load_table(args$input_tab)
  vcf_header <- unlist(read_header(args$input_vcf, prefix="##"))

  # extract purity, ploidy from header
  purity <- extract_from_header(vcf_header, "purity")
  ploidy <- extract_from_header(vcf_header, "ploidy")
  dipLogR <- extract_from_header(vcf_header, "dipLogR")
  est_insert_size <- extract_from_header(vcf_header, "est_insert_size")

  # undo renaming by cnv_facets to have only numeric chromosome names
  df_cnv_tab[df_cnv_tab$chrom == 'X', "chrom"] <- "23"
  df_cnv_tab[df_cnv_tab$chrom == 'Y', "chrom"] <- "24"
  df_cnv_tab$chrom <- as.numeric(df_cnv_tab$chrom)

  # compute chromsome arm CNA
  out_arm_level <- arm_level_changes(df_cnv_tab, ploidy, genome=args$genome, algorithm="em")
  df_chr_arm <- out_arm_level$full_output

  # extract CNA scores from facetsSuite ================================================================================
  out_genome_doubled <- out_arm_level$genome_doubled
  out_fraction_cna <- out_arm_level$fraction_cna
  out_weighted_fraction_cna <- out_arm_level$weighted_fraction_cna
  out_aneuploidy_score <- out_arm_level$aneuploidy_score

  # loh
  # needs to capture $jointseg table from facets::procSample output, which is not done by cnv_facets
  # out_loh <- calculate_loh(df_cnv_tab, df_snps, ploidy, genome=args$genome, algorithm="em")

  # ntai
  # only segments with at least 250 probes are considered
  # a segment displays allelic imbalance if major copy number (mcn) != lower copy number (lcn)
  # for each autosome,
  # - determine if
  #   * first segment of chromosome is AI and does not extend to centromere --> telomeric AI
  #   or
  #   * last segment of chromosome is AI and starts beyond the centromere --> telomeric AI
  # - determine if the chromosome has only one segment which is AI --> chromosomal AI
  # - all other segments under AI --> interstitial AI
  # 
  # metrics
  # - ntelomeric_ai: number of chr segment starts and ends that are telomeric AI. range 0-44 (or 39?).
  # - ninterstitial_ai: number of chr segments that are interstitial AI. range 0-inf
  # - ncentromeric_ai: number of chromosomes that are AI. range 0-22
  # - ntelomeric_loh: number of chr segment starts and ends that are telomeric AI and LOH (lcn=0). range 0-44 (or 39?).
  # - ninterstitial_ai: number of chr segments that are interstitial AI and LOH (lcn=0). range 0-inf
  # - ncentromeric_ai: number of chromosomes that are AI and LOH (lcn=0). range 0-22
  out_ntai <- calculate_ntai(df_cnv_tab, ploidy, genome=args$genome, algorithm="em", min_size=0, min_probes=250)

  # lst
  # large-scale state transitions (LST) genome wide (the number of CNV breakpoints >10Mb)
  #  - consider only autosomes (1:22)
  #  - smoothen segments less than 3Mb
  #  - LST event where 2 consecutive segments each larger than 10 Mb are less than 3 Mb apart
  out_lst <- calculate_lst_custom(segs=df_cnv_tab, ploidy, genome=args$genome, algorithm="em", min_size=10e6)

  # hrdloh
  # returns the following statistic: number of segments that
  #  - are larger than 15 Mb
  #  - are in LOH but not in homozygyous deletion (tcn!=0, lcn=0; lcn!=NA)
  #  - are on chromsomes that have not completely lost one parental copy (all lcn=0 for segments on the chr)
  #  - are on autosomes (1:22)
  out_hrdloh <- calculate_hrdloh(df_cnv_tab, ploidy, algorithm="em")

  # compute in-house CNA scores ========================================================================================
  genome <- extract_genome_sizes(vcf_header)

  out_genome_fractions <- calculate_genome_fractions(df_cnv_tab, genome)
  out_wgd <- calculate_wgd(df_cnv_tab, genome)

  # build table of summary statistics
  tumor_sample <- gsub("_vs_.*$", "", basename(args$input_vcf))
  normal_sample <- gsub(".vcf.gz$", "", gsub("^.*_vs_", "", basename(args$input_vcf)))

  # build table of summary statistics
  df_cna_sum <- data.frame(Tumor_Sample_Barcode=tumor_sample,
                           Matched_Norm_Sample_Barcode=normal_sample,
                           Ploidy=ploidy,
                           WGD=out_wgd$wgd, check.names=F)
  df_cna_sum <- bind_cols(df_cna_sum, data.frame(out_genome_fractions, check.names=F))
  df_cna_sum <- bind_cols(df_cna_sum,
                          data.frame(genome_doubled=ifelse(out_genome_doubled, 1, 0),
                                     fraction_cna=out_fraction_cna,
                                     weighted_fraction_cna=out_weighted_fraction_cna,
                                     aneuploidy_score=out_aneuploidy_score,
                                     lst=out_lst,
                                     hrd_loh=out_hrdloh,
                                     check.names=F))
  df_cna_sum <- bind_cols(df_cna_sum, data.frame(out_ntai, check.names=F))


  # save results =======================================================================================================
  write.table(df_chr_arm, file=args$output_arm, row.names=F, sep="\t", quote=F)
  write.table(df_cna_sum, file=args$output_sum, row.names=F, sep="\t", quote=F)
}

# run ==================================================================================================================

if (getOption('run.main', default=TRUE)) {
  parser <- ArgumentParser(description='Call chromosome arm copy-number changes.')
  parser$add_argument("--input_tab", type="character", help="Path to table parsed from vcf of cnv_facets.")
  parser$add_argument("--input_vcf", type="character", help="Path to VCF from cnv_facets.")
  parser$add_argument("--genome", type="character", help="Genome build.", default="hg19")
  parser$add_argument("--output_arm", type="character", help="Path to output table of chromosome arm CNVs.")
  parser$add_argument("--output_sum", type="character", help="Path to output table CNV summary statistics.")
  parser$add_argument('--log', type="character", help='Path to log file.')
  args <- parser$parse_args()

  # log file
  log <- file(args$log, open="wt")
  sink(log)
  sink(log, type="message")

  print(args)
  main(args)
}
