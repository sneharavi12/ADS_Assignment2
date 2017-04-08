library(ggplot2)
library(dplyr)
library(plotly)
library(ggplot2)
library(choroplethr)
library(choroplethrMaps)

data1<-read.csv("G:/ads_assignment_2/Data/Cleaned/cleaned_loandata.csv",stringsAsFactors = FALSE)
dim(data1)
colnames(data1)

#Loan amount Distribution based on Grades assigned by Lending Club
#Those with higher grades (A, B, C and D) have received more loans compared to those with lower grades (E, F and G)
library(ggplot2)
ggplot(data1, aes(loan_amnt, col = grade)) + geom_histogram(bins = 50) + facet_grid(grade ~ .)


#Exploring interest rates based on Grades assigned by Lending Club
#Grades are assigned based on risk, and so interest rates go up as the risk goes up
library(ggplot2)
ggplot(data1, aes(int_rate, fill = grade)) + geom_density() + facet_grid(grade ~ .)

#Total loan issued over the years [2007 - 2015]
install.packages("lubridate")
library(lubridate)
data1$issue_d <- dmy(paste0("01-",data1$issue_d))
loan_amnt_by_month <- aggregate(loan_amnt ~ issue_d, data = data1, sum)
ggplot(loan_amnt_by_month, aes(issue_d, loan_amnt)) + geom_bar(stat = "identity")

#Total loan amount for each loan status
loan_amnt_by_status <- aggregate(loan_amnt ~ loan_status, data = data1, sum)
ggplot(loan_amnt_by_status, aes(loan_status, loan_amnt, fill = loan_status)) + geom_bar(stat = "identity") + scale_x_discrete(breaks=NULL)


#Paid Vs Unpaid
#Creating a new column with 2 factor levels
#- "Paid/current" - Represents the status is Current or Fully Paid
#- "Other" - Represents defaults, chargeroff and other status
#Exploring Unpaid loans
data1$paidVsUnpaid <- "Other"
data1$paidVsUnpaid[which(data1$loan_status == "Fully Paid" | data1$loan_status == "Current" | data1$loan_status == "Does not meet the credit policy. Status:Fully Paid") ] <- "Paid/current"
data1$paidVsUnpaid <- factor(data1$paidVsUnpaid)
data1$paidVsUnpaid <- factor(data1$paidVsUnpaid, levels = rev(levels(data1$paidVsUnpaid)))
table(data1$paidVsUnpaid)
ggplot(data1, aes(paidVsUnpaid, loan_amnt, fill = paidVsUnpaid)) + geom_bar(stat = "identity")

#Paid Vs. Unpaid loan amount over the Grades
loan_by_grade <- aggregate(loan_amnt ~ sub_grade + paidVsUnpaid, data = data1, sum)
gbar <- ggplot(loan_by_grade, aes(sub_grade, loan_amnt, fill = paidVsUnpaid)) 
gbar + geom_bar(stat = "identity") + theme(axis.text.x=element_text(size=7))


#checking the distribution of loan amounts by status.
library(ggplot2)
box_status <- ggplot(data1, aes(loan_status, loan_amnt))
box_status + geom_boxplot(aes(fill = loan_status)) +
  theme(axis.text.x = element_blank()) +
  labs(list(
    title = "Loan amount by status",
    x = "Status",
    y = "Amount")) 

library(dplyr)
#value of loans of different grades was changing over time
amnt_df_grade <- data1 %>% 
  select(issue_d, loan_amnt, grade) %>% 
  group_by(issue_d, grade) %>% 
  summarise(Amount = sum(loan_amnt))

ts_amnt_grade <- ggplot(amnt_df_grade, 
                        aes(x = issue_d, y = Amount))
ts_amnt_grade + geom_area(aes(fill=grade)) + xlab("Date issued")