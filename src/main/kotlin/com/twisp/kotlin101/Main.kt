package com.twisp.kotlin101

import com.twisp.core.v1.AnyRequest
import com.twisp.core.v1.AnyRequestOperation
import com.twisp.core.v1.AnyResponse
import com.twisp.core.v1.AnyResponseOperation
import com.twisp.core.v1.BeginTransactionRequest
import com.twisp.core.v1.CommitTransactionRequest
import com.twisp.core.v1.CreateAccountRequest
import com.twisp.core.v1.CreateAccountSetRequest
import com.twisp.core.v1.CreateJournalRequest
import com.twisp.core.v1.CreateTranCodeRequest
import com.twisp.core.v1.DebitOrCredit
import com.twisp.core.v1.ListAccountSetMembersRequest
import com.twisp.core.v1.ListBalanceHistoryRequest
import com.twisp.core.v1.Paginate
import com.twisp.core.v1.PostTransactionRequest
import com.twisp.core.v1.PostTransactionResponse
import com.twisp.core.v1.ReadAccountRequest
import com.twisp.core.v1.ReadAccountSetRequest
import com.twisp.core.v1.ReadBalanceRequest
import com.twisp.core.v1.RecordInfo
import com.twisp.core.v1.SortOrder
import com.twisp.core.v1.TranCodeEntry
import com.twisp.core.v1.TranCodeParam
import com.twisp.core.v1.TranCodeTransaction
import com.twisp.core.v1.AnyServiceGrpcKt
import com.twisp.core.v1.CreateAccountSetResponse
import com.twisp.core.v1.CreateAccountResponse
import com.twisp.core.v1.CreateJournalResponse
import com.twisp.core.v1.CreateTranCodeResponse
import com.twisp.core.v1.PostTransactionResponseOrBuilder
import com.twisp.core.v1.ReadAccountResponse
import com.twisp.core.v1.ReadAccountSetResponse
import com.twisp.core.v1.ReadBalanceResponse
import com.twisp.core.v1.ListAccountSetMembersResponse
import com.twisp.core.v1.Account
import com.twisp.core.v1.AccountSet
import com.twisp.core.v1.AccountSetMemberType
import com.twisp.core.v1.Balance
import com.twisp.core.v1.Entry
import com.twisp.core.v1.ListBalanceHistoryResponse
import com.twisp.core.v1.ListBalanceHistoryResponse.Edge
import com.twisp.type.v1.Money
import com.twisp.type.v1.Type
import com.twisp.type.v1.UUID as TwispUUID
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.Metadata.ASCII_STRING_MARSHALLER
import io.grpc.stub.MetadataUtils
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

private const val TWISP_ACCOUNT_HEADER = "x-twisp-account-id"
private const val AUTH_HEADER = "Authorization"

private val JOURNAL_ID = UUID.fromString("822cb59f-ce51-4837-8391-2af3b7a5fc51")
private val ERNIE_ACCOUNT_ID = UUID.fromString("1fd1dd3e-33fe-4ef5-9d58-676ef8d306b5")
private val BERT_ACCOUNT_ID = UUID.fromString("6c6affb0-5cf5-402b-8d84-01bfc1624a2c")
private val ASSETS_ACCOUNT_ID = UUID.fromString("78551b96-9c34-46f9-8d5f-c86e4459fcd7")
private val REVENUE_ACCOUNT_ID = UUID.fromString("ece5e752-5445-4f4e-8861-d09c5c417061")
private val ACH_CREDIT_TRAN_CODE_ID = UUID.fromString("45f3f5da-034e-40c1-aaff-ab6d01bd446f")
private val ACH_DEBIT_TRAN_CODE_ID = UUID.fromString("fab492ae-2fe4-4fcd-9bf7-cf06eb5f796b")
private val BANK_TRANSFER_TRAN_CODE_ID = UUID.fromString("a0d9e35a-1df6-4f22-8e39-15c72e60b2d5")
private val DEPOSIT_TRANSACTION_ID = UUID.fromString("42847c7f-1972-4448-91b7-652c378760f4")
private val WITHDRAWAL_TRANSACTION_ID = UUID.fromString("39d2288d-96f9-40c7-b587-e7e75df083fa")
private val BANK_TRANSFER_TRANSACTION_ID = UUID.fromString("9c328550-bba3-423b-a58a-b3f9786a80ae")
private val CUSTOMERS_ACCOUNT_SET_ID = UUID.fromString("a6ee5252-a8db-4fdc-960d-64970f3385ab")

fun main() = runBlocking {
    val channel = ManagedChannelBuilder.forAddress("localhost", 8081)
        .usePlaintext()
        .build()

    val metadata = Metadata().apply {
        put(Metadata.Key.of(TWISP_ACCOUNT_HEADER, ASCII_STRING_MARSHALLER), "000000000000")
        put(Metadata.Key.of(AUTH_HEADER, ASCII_STRING_MARSHALLER), "Bearer <a real token here if in cloud>")
    }

    val stub = AnyServiceGrpcKt.AnyServiceCoroutineStub(channel)
    val runner = TutorialRunner(stub, metadata, this)

    val actions = listOf(
        TutorialAction("001 Create General Ledger Journal") { createJournal(it) },
        TutorialAction("002 Create Customer Accounts") { createCustomerAccounts(it) },
        TutorialAction("003 Create Assets Account") { createAssetsAccount(it) },
        TutorialAction("004 Check Initial Balances") { checkAccountBalances(it) },
        TutorialAction("005 Create ACH Deposit/Withdrawal TranCodes") { createAchTranCodes(it) },
        TutorialAction("006 Post ACH Deposit") { postDeposit(it) },
        TutorialAction("007 Post ACH Withdrawal") { postWithdrawal(it) },
        TutorialAction("008 Create Revenue Account") { createRevenueAccount(it) },
        TutorialAction("009 Create Bank Transfer TranCode") { createBankTransferTranCode(it) },
        TutorialAction("012 Post Bank Transfer") { postBankTransfer(it) },
        TutorialAction("010 Create Customers Account Set") { createCustomersAccountSet(it) },
        TutorialAction("011 Add Customers To Account Set") { addCustomersToSet(it) },
        TutorialAction("013 Get Customers Balances") { getCustomersBalances(it) },
        TutorialAction("014 Get Ernie Balance History (uses multiple stream interactions)") { getErnieBalanceHistory(it) },
    )

    try {
        actions.forEach { action ->
            println("\n== ${action.title} ==")
            action.run(runner)
        }
    } finally {
        runner.close()
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}

private data class TutorialAction(
    val title: String,
    val run: suspend (TutorialRunner) -> Unit,
)

private data class EntryRecord(
    val entry: Entry,
    val transactionId: UUID?,
    val postedAt: Instant,
    val order: Int,
)

private class TutorialRunner(
    stub: AnyServiceGrpcKt.AnyServiceCoroutineStub,
    metadata: Metadata,
    scope: CoroutineScope,
) {
    private val stream = StreamClient(stub, metadata, scope)

    val accounts: MutableMap<UUID, Account> = mutableMapOf()
    val accountBalances: MutableMap<UUID, Balance> = mutableMapOf()
    val accountEntries: MutableMap<UUID, MutableList<EntryRecord>> = mutableMapOf()
    var entryOrder: Int = 0
    var customersAccountSetAccountId: UUID? = null
    var customersNormalBalanceType: DebitOrCredit = DebitOrCredit.DEBIT_OR_CREDIT_CREDIT

    suspend fun send(request: AnyRequest): AnyResponse = stream.send(request)

    fun close() {
        stream.close()
    }
}

private class StreamClient(
    private val stub: AnyServiceGrpcKt.AnyServiceCoroutineStub,
    private val metadata: Metadata,
    scope: CoroutineScope,
) {
    private val requests = Channel<AnyRequest>(Channel.UNLIMITED)
    private val responses = Channel<AnyResponse>(Channel.UNLIMITED)
    private val job: Job

    init {
        job = scope.launch(Dispatchers.IO) {
            try {
                stub.anyStream(requests.receiveAsFlow(), metadata).collect { response ->
                    responses.send(response)
                }
            } catch (ex: Throwable) {
                responses.close(ex)
            } finally {
                responses.close()
            }
        }
    }

    suspend fun send(request: AnyRequest): AnyResponse {
        requests.send(request)
        val result = responses.receiveCatching()
        result.exceptionOrNull()?.let { throw it }
        return result.getOrNull() ?: error("twisp any stream closed")
    }

    fun close() {
        requests.close()
        job.cancel()
    }
}

private suspend fun createJournal(runner: TutorialRunner) {
    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setCreateJournal(
                    CreateJournalRequest.newBuilder()
                        .setJournalId(JOURNAL_ID.toProtoUuid())
                        .setName("General Ledger")
                        .setDescription("Primary journal for Zuzu.")
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    val response = runner.send(request)
    val createJournalOp = response.operationsList.firstOrNull { it.hasCreateJournal() }
        ?: error("journal creation response missing journal")
    val journal = createJournalOp.createJournal.journal
    val journalId = journal.journalId.toJavaUuid()
    println("Created journal ${journal.name} ($journalId)")
}

private suspend fun createCustomerAccounts(runner: TutorialRunner) {
    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setCreateAccount(
                    CreateAccountRequest.newBuilder()
                        .setAccountId(ERNIE_ACCOUNT_ID.toProtoUuid())
                        .setName("Ernie Bishop - Checking")
                        .setCode("ERNIE.CHECKING")
                        .setDescription("Ernie's checking account")
                        .setNormalBalanceType(DebitOrCredit.DEBIT_OR_CREDIT_CREDIT)
                        .build(),
                )
                .build(),
        )
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setCreateAccount(
                    CreateAccountRequest.newBuilder()
                        .setAccountId(BERT_ACCOUNT_ID.toProtoUuid())
                        .setName("Bert - Checking")
                        .setCode("BERT.CHECKING")
                        .setDescription("Bert's checking account")
                        .setNormalBalanceType(DebitOrCredit.DEBIT_OR_CREDIT_CREDIT)
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    val response = runner.send(request)
    val accountOps = response.operationsList.filter { it.hasCreateAccount() }
    if (accountOps.size < 2) {
        error("account creation response missing accounts")
    }
    val ids = listOf(ERNIE_ACCOUNT_ID, BERT_ACCOUNT_ID)
    accountOps.zip(ids).forEach { (op, id) ->
        val account = op.createAccount.account
        runner.recordAccount(account)
        println("Created account ${account.name} ($id)")
    }
}

private suspend fun createAssetsAccount(runner: TutorialRunner) {
    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setCreateAccount(
                    CreateAccountRequest.newBuilder()
                        .setAccountId(ASSETS_ACCOUNT_ID.toProtoUuid())
                        .setName("Assets")
                        .setCode("ASSET")
                        .setDescription("Zuzu's assets (e.g. cash deposits)")
                        .setNormalBalanceType(DebitOrCredit.DEBIT_OR_CREDIT_DEBIT_UNSPECIFIED)
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    val response = runner.send(request)
    val accountOp = response.operationsList.firstOrNull { it.hasCreateAccount() }
        ?: error("assets account missing in response")
    val account = accountOp.createAccount.account
    runner.recordAccount(account)
    println("Created assets account ${account.name} ($ASSETS_ACCOUNT_ID)")
}

private suspend fun checkAccountBalances(runner: TutorialRunner) {
    val accounts = listOf(ERNIE_ACCOUNT_ID, BERT_ACCOUNT_ID, ASSETS_ACCOUNT_ID)
    val requestBuilder = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
    accounts.forEach { id ->
        requestBuilder.addOperations(
            AnyRequestOperation.newBuilder()
                .setReadBalance(
                    ReadBalanceRequest.newBuilder()
                        .setJournalId(JOURNAL_ID.toProtoUuid())
                        .setAccountId(id.toProtoUuid())
                        .setCurrency("USD")
                        .build(),
                )
                .build(),
        )
    }
    requestBuilder.addOperations(commitTransactionOp())

    val response = runner.send(requestBuilder.build())
    val balanceOps = response.operationsList.filter { it.hasReadBalance() }
    if (balanceOps.size < accounts.size) {
        error("balance response missing entries")
    }

    println("Settled normal balances (USD):")
    balanceOps.zip(accounts).forEach { (op, accountId) ->
        val balance = op.readBalance.balance
        runner.recordBalance(accountId, balance)
        val account = runner.accounts[accountId]
        val name = account?.name ?: "Account $accountId"
        val value = runner.normalBalanceString(
            accountId,
            balance,
            DebitOrCredit.DEBIT_OR_CREDIT_DEBIT_UNSPECIFIED,
        )
        println("- $name: $value")
    }
}

private suspend fun createAchTranCodes(runner: TutorialRunner) {
    val depositDescription = "An ACH credit into a customer account."
    val withdrawDescription = "An ACH debit into a customer account."
    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setCreateTranCode(
                    CreateTranCodeRequest.newBuilder()
                        .setTranCodeId(ACH_CREDIT_TRAN_CODE_ID.toProtoUuid())
                        .setCode("ACH_CREDIT")
                        .setDescription(depositDescription)
                        .addParams(
                            TranCodeParam.newBuilder()
                                .setName("account")
                                .setTypeValue(Type.TYPE_UUID_VALUE)
                                .setDescription("Deposit account ID.")
                                .build(),
                        )
                        .addParams(
                            TranCodeParam.newBuilder()
                                .setName("amount")
                                .setTypeValue(Type.TYPE_DECIMAL_VALUE)
                                .setDescription("Amount with decimal, e.g. `1.23`.")
                                .build(),
                        )
                        .addParams(
                            TranCodeParam.newBuilder()
                                .setName("effective")
                                .setTypeValue(Type.TYPE_DATE_VALUE)
                                .setDescription("Effective date for transaction.")
                                .build(),
                        )
                        .setTransaction(
                            TranCodeTransaction.newBuilder()
                                .setJournalId("uuid('$JOURNAL_ID')")
                                .setEffective("params.effective")
                                .build(),
                        )
                        .addEntries(
                            TranCodeEntry.newBuilder()
                                .setAccountId("uuid('$ASSETS_ACCOUNT_ID')")
                                .setUnits("params.amount")
                                .setCurrency("'USD'")
                                .setEntryType("'ACH_DR'")
                                .setDirection("DEBIT")
                                .setLayer("SETTLED")
                                .build(),
                        )
                        .addEntries(
                            TranCodeEntry.newBuilder()
                                .setAccountId("params.account")
                                .setUnits("params.amount")
                                .setCurrency("'USD'")
                                .setEntryType("'ACH_CR'")
                                .setDirection("CREDIT")
                                .setLayer("SETTLED")
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setCreateTranCode(
                    CreateTranCodeRequest.newBuilder()
                        .setTranCodeId(ACH_DEBIT_TRAN_CODE_ID.toProtoUuid())
                        .setCode("ACH_DEBIT")
                        .setDescription(withdrawDescription)
                        .addParams(
                            TranCodeParam.newBuilder()
                                .setName("account")
                                .setTypeValue(Type.TYPE_UUID_VALUE)
                                .setDescription("Withdraw account ID.")
                                .build(),
                        )
                        .addParams(
                            TranCodeParam.newBuilder()
                                .setName("amount")
                                .setTypeValue(Type.TYPE_DECIMAL_VALUE)
                                .setDescription("Amount with decimal, e.g. `1.23`.")
                                .build(),
                        )
                        .addParams(
                            TranCodeParam.newBuilder()
                                .setName("effective")
                                .setTypeValue(Type.TYPE_DATE_VALUE)
                                .setDescription("Effective date for transaction.")
                                .build(),
                        )
                        .setTransaction(
                            TranCodeTransaction.newBuilder()
                                .setJournalId("uuid('$JOURNAL_ID')")
                                .setEffective("params.effective")
                                .build(),
                        )
                        .addEntries(
                            TranCodeEntry.newBuilder()
                                .setAccountId("uuid('$ASSETS_ACCOUNT_ID')")
                                .setUnits("params.amount")
                                .setCurrency("'USD'")
                                .setEntryType("'ACH_CR'")
                                .setDirection("CREDIT")
                                .setLayer("SETTLED")
                                .build(),
                        )
                        .addEntries(
                            TranCodeEntry.newBuilder()
                                .setAccountId("params.account")
                                .setUnits("params.amount")
                                .setCurrency("'USD'")
                                .setEntryType("'ACH_DR'")
                                .setDirection("DEBIT")
                                .setLayer("SETTLED")
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    runner.send(request)
    println("Created ACH_CREDIT and ACH_DEBIT tran codes.")
}

private suspend fun postDeposit(runner: TutorialRunner) {
    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setPostTransaction(
                    PostTransactionRequest.newBuilder()
                        .setTransactionId(DEPOSIT_TRANSACTION_ID.toProtoUuid())
                        .setTranCode("ACH_CREDIT")
                        .putParams("account", ERNIE_ACCOUNT_ID.toString())
                        .putParams("amount", "9.53")
                        .putParams("effective", "2022-09-21")
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    val response = runner.send(request)
    val postOp = response.operationsList.firstOrNull { it.hasPostTransaction() }
        ?: error("deposit response missing transaction")
    val postResponse = postOp.postTransaction
    runner.recordEntries(postResponse)
    val txId = postResponse.transaction.transactionId.toJavaUuid()
    println("Posted ACH_CREDIT deposit (transaction $txId) amount 9.53 USD.")
}

private suspend fun postWithdrawal(runner: TutorialRunner) {
    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setPostTransaction(
                    PostTransactionRequest.newBuilder()
                        .setTransactionId(WITHDRAWAL_TRANSACTION_ID.toProtoUuid())
                        .setTranCode("ACH_DEBIT")
                        .putParams("account", ERNIE_ACCOUNT_ID.toString())
                        .putParams("amount", "4.28")
                        .putParams("effective", "2022-09-21")
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    val response = runner.send(request)
    val postOp = response.operationsList.firstOrNull { it.hasPostTransaction() }
        ?: error("withdrawal response missing transaction")
    val postResponse = postOp.postTransaction
    runner.recordEntries(postResponse)
    val txId = postResponse.transaction.transactionId.toJavaUuid()
    println("Posted ACH_DEBIT withdrawal (transaction $txId) amount 4.28 USD.")
}

private suspend fun createRevenueAccount(runner: TutorialRunner) {
    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setCreateAccount(
                    CreateAccountRequest.newBuilder()
                        .setAccountId(REVENUE_ACCOUNT_ID.toProtoUuid())
                        .setName("Revenues")
                        .setCode("REV")
                        .setDescription("Company revenues (e.g. fees)")
                        .setNormalBalanceType(DebitOrCredit.DEBIT_OR_CREDIT_CREDIT)
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    val response = runner.send(request)
    val accountOp = response.operationsList.firstOrNull { it.hasCreateAccount() }
        ?: error("revenue account missing in response")
    val account = accountOp.createAccount.account
    runner.recordAccount(account)
    println("Created revenue account ${account.name} ($REVENUE_ACCOUNT_ID)")
}

private suspend fun createBankTransferTranCode(runner: TutorialRunner) {
    val description =
        "Transfer \$ internally from one checking account to another. The sender is charged a 1% fee or \$10, whichever is smaller."
    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setCreateTranCode(
                    CreateTranCodeRequest.newBuilder()
                        .setTranCodeId(BANK_TRANSFER_TRAN_CODE_ID.toProtoUuid())
                        .setCode("BANK_TRANSFER")
                        .setDescription(description)
                        .addParams(
                            TranCodeParam.newBuilder()
                                .setName("fromAccount")
                                .setTypeValue(Type.TYPE_UUID_VALUE)
                                .setDescription("Sender's account ID.")
                                .build(),
                        )
                        .addParams(
                            TranCodeParam.newBuilder()
                                .setName("toAccount")
                                .setTypeValue(Type.TYPE_UUID_VALUE)
                                .setDescription("Receiver's account ID.")
                                .build(),
                        )
                        .addParams(
                            TranCodeParam.newBuilder()
                                .setName("amount")
                                .setTypeValue(Type.TYPE_DECIMAL_VALUE)
                                .setDescription("Amount with decimal, e.g. `1.23`.")
                                .build(),
                        )
                        .addParams(
                            TranCodeParam.newBuilder()
                                .setName("fee")
                                .setTypeValue(Type.TYPE_DECIMAL_VALUE)
                                .setDescription("Transfer fee as decimal percentage, e.g. `0.01` for 1%")
                                .build(),
                        )
                        .addParams(
                            TranCodeParam.newBuilder()
                                .setName("effective")
                                .setTypeValue(Type.TYPE_DATE_VALUE)
                                .setDescription("Effective date for transaction.")
                                .build(),
                        )
                        .setTransaction(
                            TranCodeTransaction.newBuilder()
                                .setJournalId("uuid('$JOURNAL_ID')")
                                .setEffective("params.effective")
                                .build(),
                        )
                        .addEntries(
                            TranCodeEntry.newBuilder()
                                .setAccountId("uuid(params.fromAccount)")
                                .setUnits("params.amount")
                                .setCurrency("'USD'")
                                .setEntryType("'TRANSFER_DR'")
                                .setDirection("DEBIT")
                                .setLayer("SETTLED")
                                .build(),
                        )
                        .addEntries(
                            TranCodeEntry.newBuilder()
                                .setAccountId("uuid(params.toAccount)")
                                .setUnits("params.amount")
                                .setCurrency("'USD'")
                                .setEntryType("'TRANSFER_CR'")
                                .setDirection("CREDIT")
                                .setLayer("SETTLED")
                                .build(),
                        )
                        .addEntries(
                            TranCodeEntry.newBuilder()
                                .setAccountId("uuid(params.fromAccount)")
                                .setUnits("decimal.Round(decimal.Mul(params.amount, params.fee), 'half_up', 2)")
                                .setCurrency("'USD'")
                                .setEntryType("'TRANSFER_FEE_DR'")
                                .setDirection("DEBIT")
                                .setLayer("SETTLED")
                                .build(),
                        )
                        .addEntries(
                            TranCodeEntry.newBuilder()
                                .setAccountId("uuid('$REVENUE_ACCOUNT_ID')")
                                .setUnits("decimal.Round(decimal.Mul(params.amount, params.fee), 'half_up', 2)")
                                .setCurrency("'USD'")
                                .setEntryType("'TRANSFER_FEE_CR'")
                                .setDirection("CREDIT")
                                .setLayer("SETTLED")
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    runner.send(request)
    println("Created BANK_TRANSFER tran code.")
}

private suspend fun postBankTransfer(runner: TutorialRunner) {
    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setPostTransaction(
                    PostTransactionRequest.newBuilder()
                        .setTransactionId(BANK_TRANSFER_TRANSACTION_ID.toProtoUuid())
                        .setTranCode("BANK_TRANSFER")
                        .putParams("fromAccount", ERNIE_ACCOUNT_ID.toString())
                        .putParams("toAccount", BERT_ACCOUNT_ID.toString())
                        .putParams("amount", "2.25")
                        .putParams("fee", "0.02")
                        .putParams("effective", "2022-09-10")
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    val response = runner.send(request)
    val postOp = response.operationsList.firstOrNull { it.hasPostTransaction() }
        ?: error("bank transfer response missing transaction")
    val postResponse = postOp.postTransaction
    runner.recordEntries(postResponse)
    val txId = postResponse.transaction.transactionId.toJavaUuid()
    println("Posted BANK_TRANSFER (transaction $txId) amount 2.25 USD with fee 0.05 USD.")
}

private suspend fun createCustomersAccountSet(runner: TutorialRunner) {
    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setCreateAccountSet(
                    CreateAccountSetRequest.newBuilder()
                        .setAccountSetId(CUSTOMERS_ACCOUNT_SET_ID.toProtoUuid())
                        .setJournalId(JOURNAL_ID.toProtoUuid())
                        .setName("Customers")
                        .setDescription("All customer's accounts")
                        .setNormalBalanceType(DebitOrCredit.DEBIT_OR_CREDIT_CREDIT)
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    val response = runner.send(request)
    val op = response.operationsList.firstOrNull { it.hasCreateAccountSet() }
        ?: error("account set missing in response")
    val accountSet = op.createAccountSet.accountSet
    runner.customersAccountSetAccountId = accountSet.accountId.toJavaUuid()
    runner.customersNormalBalanceType = DebitOrCredit.DEBIT_OR_CREDIT_CREDIT
    println("Created account set ${accountSet.name} ($CUSTOMERS_ACCOUNT_SET_ID)")
}

private suspend fun addCustomersToSet(runner: TutorialRunner) {
    val makeOp: (UUID) -> AnyRequestOperation = { accountId ->
        AnyRequestOperation.newBuilder()
            .setAddAccountSetMember(
                com.twisp.core.v1.AddAccountSetMemberRequest.newBuilder()
                    .setAccountSetId(CUSTOMERS_ACCOUNT_SET_ID.toProtoUuid())
                    .setMemberType(AccountSetMemberType.ACCOUNT_SET_MEMBER_TYPE_ACCOUNT_UNSPECIFIED)
                    .setMemberId(accountId.toProtoUuid())
                    .build(),
            )
            .build()
    }

    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(makeOp(ERNIE_ACCOUNT_ID))
        .addOperations(makeOp(BERT_ACCOUNT_ID))
        .addOperations(commitTransactionOp())
        .build()

    runner.send(request)
    println("Added Ernie and Bert accounts to Customers account set.")
}

private suspend fun getCustomersBalances(runner: TutorialRunner) {
    val accountSetAccountId = runner.customersAccountSetAccountId
        ?: error("customers account set account id not recorded")

    val filters = ListAccountSetMembersRequest.Filters.newBuilder()
        .setAccountSetId(
            com.twisp.core.v1.FilterValue.newBuilder()
                .setEq(CUSTOMERS_ACCOUNT_SET_ID.toString())
                .build(),
        )
        .build()

    val request = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setReadAccountSet(
                    ReadAccountSetRequest.newBuilder()
                        .setAccountSetId(CUSTOMERS_ACCOUNT_SET_ID.toProtoUuid())
                        .setJournalId(JOURNAL_ID.toProtoUuid())
                        .build(),
                )
                .build(),
        )
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setListAccountSetMembers(
                    ListAccountSetMembersRequest.newBuilder()
                        .setIndex(ListAccountSetMembersRequest.Index.INDEX_MEMBER_ID)
                        .setPaging(Paginate.newBuilder().setFirst(10).build())
                        .setWhere(filters)
                        .build(),
                )
                .build(),
        )
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setReadBalance(
                    ReadBalanceRequest.newBuilder()
                        .setJournalId(JOURNAL_ID.toProtoUuid())
                        .setAccountId(accountSetAccountId.toProtoUuid())
                        .setCurrency("USD")
                        .build(),
                )
                .build(),
        )
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setReadBalance(
                    ReadBalanceRequest.newBuilder()
                        .setJournalId(JOURNAL_ID.toProtoUuid())
                        .setAccountId(ERNIE_ACCOUNT_ID.toProtoUuid())
                        .setCurrency("USD")
                        .build(),
                )
                .build(),
        )
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setReadBalance(
                    ReadBalanceRequest.newBuilder()
                        .setJournalId(JOURNAL_ID.toProtoUuid())
                        .setAccountId(BERT_ACCOUNT_ID.toProtoUuid())
                        .setCurrency("USD")
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    val response = runner.send(request)
    val accountSetOp = response.operationsList.firstOrNull { it.hasReadAccountSet() }
        ?: error("account set not returned")
    val accountSet = accountSetOp.readAccountSet.accountSet
    println("Account set ${accountSet.name} members:")

    val membersOp = response.operationsList.firstOrNull { it.hasListAccountSetMembers() }
    membersOp?.listAccountSetMembers?.edgesList?.forEach { edge ->
        val node = edge.node ?: return@forEach
        val memberId = node.memberId.toJavaUuid()
        val accountName = runner.accounts[memberId]?.name ?: "Account $memberId"
        println("- $accountName ($memberId)")
    }

    val balanceOps = response.operationsList.filter { it.hasReadBalance() }
    if (balanceOps.size < 3) {
        error("expected balances for account set and members")
    }
    val aggregateBalance = balanceOps[0].readBalance.balance
    val ernieBalance = balanceOps[1].readBalance.balance
    val bertBalance = balanceOps[2].readBalance.balance

    runner.recordBalance(accountSetAccountId, aggregateBalance)
    runner.recordBalance(ERNIE_ACCOUNT_ID, ernieBalance)
    runner.recordBalance(BERT_ACCOUNT_ID, bertBalance)

    println("Customers account set balance (normal credit): ${
        runner.normalBalanceString(
            accountSetAccountId,
            aggregateBalance,
            runner.customersNormalBalanceType,
        )
    } USD")
    println("Ernie balance: ${
        runner.normalBalanceString(
            ERNIE_ACCOUNT_ID,
            ernieBalance,
            DebitOrCredit.DEBIT_OR_CREDIT_CREDIT,
        )
    } USD")
    println("Bert balance: ${
        runner.normalBalanceString(
            BERT_ACCOUNT_ID,
            bertBalance,
            DebitOrCredit.DEBIT_OR_CREDIT_CREDIT,
        )
    } USD")
}

private suspend fun getErnieBalanceHistory(runner: TutorialRunner) {
    val readRequest = AnyRequest.newBuilder()
        .addOperations(beginTransactionOp())
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setReadAccount(
                    ReadAccountRequest.newBuilder()
                        .setAccountId(ERNIE_ACCOUNT_ID.toProtoUuid())
                        .build(),
                )
                .build(),
        )
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setReadBalance(
                    ReadBalanceRequest.newBuilder()
                        .setJournalId(JOURNAL_ID.toProtoUuid())
                        .setAccountId(ERNIE_ACCOUNT_ID.toProtoUuid())
                        .setCurrency("USD")
                        .build(),
                )
                .build(),
        )
        .build()

    val readResponse = runner.send(readRequest)
    val accountOp = readResponse.operationsList.firstOrNull { it.hasReadAccount() }
    val account = accountOp?.readAccount?.account
    val accountName = account?.name ?: "Account $ERNIE_ACCOUNT_ID"
    if (account != null) {
        runner.recordAccount(account)
    }

    val balanceOp = readResponse.operationsList.firstOrNull { it.hasReadBalance() }
        ?: error("balance response missing")
    val balanceResponse = balanceOp.readBalance
    val currentBalance = balanceResponse.balance
    runner.recordBalance(ERNIE_ACCOUNT_ID, currentBalance)

    println(
        "Current settled balance for $accountName: ${
            runner.normalBalanceString(
                ERNIE_ACCOUNT_ID,
                currentBalance,
                DebitOrCredit.DEBIT_OR_CREDIT_CREDIT,
            )
        } USD",
    )

    val recordInfo = balanceResponse.recordInfo
        ?: error("balance read response missing record info")

    val historyRequest = AnyRequest.newBuilder()
        .addOperations(
            AnyRequestOperation.newBuilder()
                .setListBalanceHistory(
                    ListBalanceHistoryRequest.newBuilder()
                        .setPaging(Paginate.newBuilder().setFirst(10).build())
                        .setSort(SortOrder.SORT_ORDER_DESC)
                        .setRecordInfo(
                            RecordInfo.newBuilder()
                                .setRecordId(recordInfo.recordId)
                                .setVersion(recordInfo.version)
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .addOperations(commitTransactionOp())
        .build()

    val historyResponse = runner.send(historyRequest)
    val historyOp = historyResponse.operationsList.firstOrNull { it.hasListBalanceHistory() }
    println("Recent balance versions:")
    historyOp?.listBalanceHistory?.edgesList.orEmpty().forEach { edge ->
        val edgeRecord = edge.recordInfo ?: return@forEach
        val bal = edge.node ?: return@forEach
        val value = runner.normalBalanceString(
            ERNIE_ACCOUNT_ID,
            bal,
            DebitOrCredit.DEBIT_OR_CREDIT_CREDIT,
        )
        println("- v${edgeRecord.version} -> $value USD")
    }

    println("Recent account entries:")
    val records = runner.accountEntries[ERNIE_ACCOUNT_ID]
        ?.sortedWith(compareByDescending<EntryRecord> { it.postedAt }.thenByDescending { it.order })
        ?.take(10)
        .orEmpty()
    records.forEach { record ->
        val entry = record.entry
        println(
            "- ${directionString(entry.direction)} ${entry.entryType.padEnd(16)} ${
                formatMoney(entry.amount)
            } USD (tx ${record.transactionId})",
        )
    }
}

private fun beginTransactionOp(): AnyRequestOperation =
    AnyRequestOperation.newBuilder()
        .setBeginTransaction(BeginTransactionRequest.getDefaultInstance())
        .build()

private fun commitTransactionOp(): AnyRequestOperation =
    AnyRequestOperation.newBuilder()
        .setCommitTransaction(CommitTransactionRequest.getDefaultInstance())
        .build()

private fun TutorialRunner.recordAccount(account: Account?) {
    if (account == null) return
    val id = account.accountId.toJavaUuid() ?: return
    accounts[id] = account.toBuilder().build()
}

private fun TutorialRunner.recordBalance(accountId: UUID, balance: Balance?) {
    if (balance == null) return
    accountBalances[accountId] = balance.toBuilder().build()
}

private fun TutorialRunner.recordEntries(response: PostTransactionResponse?) {
    if (response == null) return
    val txId = response.transaction?.transactionId?.toJavaUuid()
    response.entriesList.forEach { entry ->
        val accountId = entry.accountId.toJavaUuid() ?: return@forEach
        val clone = entry.toBuilder().build()
        val postedAt = coalesceInstant(entry.committed, entry.created) ?: Instant.EPOCH
        val record = EntryRecord(clone, txId, postedAt, entryOrder++)
        accountEntries.getOrPut(accountId) { mutableListOf() }.add(record)
    }
}

private fun TutorialRunner.normalBalanceString(
    accountId: UUID,
    balance: Balance?,
    fallback: DebitOrCredit,
): String {
    val normal = accounts[accountId]?.normalBalanceType ?: fallback
    return formatNormalBalance(normal, balance)
}

private fun formatNormalBalance(
    normal: DebitOrCredit,
    balance: Balance?,
): String {
    if (balance == null || !balance.hasSettled()) {
        return "0.00"
    }
    val settled = balance.settled
    val debit = settled.drBalance.toBigDecimal()
    val credit = settled.crBalance.toBigDecimal()
    val value = if (normal == DebitOrCredit.DEBIT_OR_CREDIT_CREDIT) {
        credit.subtract(debit)
    } else {
        debit.subtract(credit)
    }
    val scale = inferExponent(balance).coerceAtLeast(0)
    return try {
        value.setScale(scale, RoundingMode.UNNECESSARY).toPlainString()
    } catch (_: ArithmeticException) {
        value.setScale(scale, RoundingMode.HALF_UP).toPlainString()
    }
}

private fun inferExponent(balance: Balance?): Int {
    if (balance == null || !balance.hasSettled()) return fractionalDigits(null)
    val settled = balance.settled
    val candidates = listOfNotNull(
        settled.takeIf { it.hasDrBalance() }?.drBalance,
        settled.takeIf { it.hasCrBalance() }?.crBalance,
    )
    return candidates.firstOrNull()?.let(::fractionalDigits) ?: fractionalDigits(null)
}

private fun Money?.toBigDecimal(): BigDecimal {
    if (this == null) return BigDecimal.ZERO
    val bytes = this.coefficient.toByteArray()
    if (bytes.isEmpty()) return BigDecimal.ZERO
    var coefficient = BigInteger(1, bytes)
    if (this.negative) {
        coefficient = coefficient.negate()
    }
    return BigDecimal(coefficient).scaleByPowerOfTen(this.exponent)
}

private fun formatMoney(money: Money?): String {
    val value = money.toBigDecimal()
    val scale = fractionalDigits(money)
    return try {
        value.setScale(scale, RoundingMode.UNNECESSARY).toPlainString()
    } catch (_: ArithmeticException) {
        value.setScale(scale, RoundingMode.HALF_UP).toPlainString()
    }
}

private fun fractionalDigits(money: Money?): Int {
    val defaultDigits = 2
    val exponent = money?.exponent ?: return defaultDigits
    return if (exponent < 0) -exponent else defaultDigits
}

private fun directionString(direction: DebitOrCredit): String =
    if (direction == DebitOrCredit.DEBIT_OR_CREDIT_CREDIT) "CREDIT" else "DEBIT"

private fun coalesceInstant(vararg timestamps: com.google.protobuf.Timestamp?): Instant? =
    timestamps.firstOrNull { it != null && (it.seconds != 0L || it.nanos != 0) }
        ?.let { Instant.ofEpochSecond(it.seconds, it.nanos.toLong()) }

private fun UUID.toProtoUuid(): TwispUUID =
    TwispUUID.newBuilder()
        .setHi(mostSignificantBits)
        .setLo(leastSignificantBits)
        .build()

private fun TwispUUID?.toJavaUuid(): UUID? {
    if (this == null) return null
    return UUID(this.hi, this.lo)
}
