Imports kCura.EventHandler
Imports Relativity.API

Public Class PopulateModificationDateAndUser
  Inherits kCura.EventHandler.PreSaveEventHandler
  Implements kCura.EventHandler.IDataEnabled

  Private Const tableName As String = "DocumentFieldEditsTrackerOpenSource"
  Private myTriggerInformation As DataTable

#Region "Required Event Handler Methods"

  Public Overrides Function Execute() As kCura.EventHandler.Response
    Dim response As New kCura.EventHandler.Response
    response.Success = True
    response.Message = String.Empty

    Dim triggerInfo As DocumentFieldEditsTrackerHolder
    Dim dbWorkspaceConnection As Relativity.API.DBContext = CType(Helper.GetDBContext(Me.Application.ArtifactID), DBContext)
    Dim dbEDDSConnection As Relativity.API.DBContext = CType(Helper.GetDBContext(-1), DBContext)

    Try
      triggerInfo = New DocumentFieldEditsTrackerHolder(dbWorkspaceConnection)

      If triggerInfo.hasData Then
        For Each triggerFieldArtifactID As Int32 In triggerInfo.htTriggerInfo.Keys

          If IsFieldOnActiveLayout(triggerFieldArtifactID) Then
            Dim currentTracker As DocumentFieldEditsTracker = CType(triggerInfo.htTriggerInfo(triggerFieldArtifactID), DocumentFieldEditsTracker)

            'Check to see if the trigger field has been modified
            'if the trigger field has changed, update the mod fields depending on whether we are tracking only the inital change or if we are tracking every change
            If HasFieldChanged(dbWorkspaceConnection, Me.ActiveArtifact.ArtifactID, currentTracker.fieldToMonitorColumnName, currentTracker.fieldToMonitorAritfactID, currentTracker.fieldTypeID, currentTracker.codeTypeID) Then
              Dim youShouldUpdateModField As Boolean = False
              'If we are only tracking the inital change, check to see if either of the two mod fields are populated.  If either one is populated, then this is not the initial change
              If currentTracker.trackInitialChangeOnly Then
                Dim sql As String
                sql = String.Format("SELECT COUNT(ArtifactID) FROM [EDDSDBO].[Document] D (NOLOCK) WHERE ArtifactID = {2} AND ([{0}] IS NOT NULL OR [{1}] IS NOT NULL)", currentTracker.reviewedOnDateFieldColumnName, currentTracker.reviewedByUserFieldColumnName, Me.ActiveArtifact.ArtifactID)
                If CType(dbWorkspaceConnection.ExecuteSqlStatementAsScalar(sql), Int32) = 0 Then
                  'It is the first modification
                  youShouldUpdateModField = True
                End If
              Else
                'We are not tracking intial change only therefore we are tracking any changes so we should update the mod fields
                youShouldUpdateModField = True
              End If

              If youShouldUpdateModField Then
                Dim dateString As String = Now.ToString("u")

                'Determine proper User Artifact ID, to accomdate for migrated cases EVN 11/29/12
                Dim userCaseUserArtifactID As Int32 = 0
                Dim sqlUserCaseUser As String
                sqlUserCaseUser = String.Format("SELECT [CaseUserArtifactID] FROM [EDDSDBO].[UserCaseUser] WITH(NOLOCK) WHERE CaseArtifactID = {0} AND [UserArtifactID] = {1}", Me.Application.ArtifactID, Me.ActiveUser.ArtifactID)
                Dim dsUserCaseUser As DataSet = dbEDDSConnection.ExecuteSqlStatementAsDataSet(sqlUserCaseUser)

                If Not IsNothing(dsUserCaseUser) Then
                  If dsUserCaseUser.Tables.Count > 0 Then
                    If dsUserCaseUser.Tables(0).Rows.Count > 0 Then
                      userCaseUserArtifactID = CType(dsUserCaseUser.Tables(0).Rows(0).Item("CaseUserArtifactID").ToString(), Int32)
                    End If
                  End If
                End If

                UpdateModFieldForDate(dbWorkspaceConnection, currentTracker.reviewedOnDateFieldColumnName, currentTracker.reviewedOnDateFieldArtifactID, dateString, Me.ActiveArtifact.ArtifactID)
                UpdateModField(dbWorkspaceConnection, currentTracker.reviewedByUserFieldColumnName, currentTracker.reviewedByUserFieldArtifactID, New Nullable(Of Int32)(userCaseUserArtifactID), Me.ActiveArtifact.ArtifactID)
              End If
            End If
          End If
        Next
      End If

    Catch ex As Exception
      response.Exception = ex
      response.Success = False
    End Try

    Return response
  End Function

  Public Overrides ReadOnly Property RequiredFields As kCura.EventHandler.FieldCollection
    Get
      Return New kCura.EventHandler.FieldCollection
    End Get
  End Property

#End Region

  Private Function IsFieldOnActiveLayout(ByVal fieldArtifactID As Int32) As Boolean
    Try
      Dim modField As kCura.EventHandler.Field
      modField = CType(Me.ActiveArtifact.Fields(fieldArtifactID), kCura.EventHandler.Field)
      If modField IsNot Nothing Then
        Return True
      Else
        Return False
      End If
    Catch ex As Exception
      Return False
    End Try
  End Function

  Private Function objectToNullableOfBoolean(ByVal value As Object) As Boolean?
    If IsDBNull(value) OrElse value Is Nothing Then
      Return New Nullable(Of Boolean)
    Else
      Return New Nullable(Of Boolean)(CType(value, Boolean))
    End If
  End Function

  Private Function HasFieldChanged(ByVal dbConnection As Relativity.API.DBContext, ByVal documentArtifactID As Int32, ByVal fieldColumnName As String, ByVal fieldArtifactID As Int32, ByVal fieldTypeID As Integer, ByVal codeTypeID As Int32?) As Boolean
    '	We handle three field types: Yes/No, single choice, and multiple choice.  both choice field types are handled in the same way.
    Dim fieldChanged As Boolean

    Select Case fieldTypeID
      Case 3 ' Yes/No field
        'Compare previous Yes/no value to current value
        Dim sql As String
        Dim objPreviousValue As Object
        Dim objCurrentValue As Object

        sql = String.Format("SELECT TOP 1 [{0}] FROM [EDDSDBO].[{2}] D WITH(NOLOCK) WHERE D.ArtifactID = {1}", fieldColumnName, documentArtifactID, "Document")
        objPreviousValue = dbConnection.ExecuteSqlStatementAsScalar(sql)
        objCurrentValue = Me.ActiveArtifact.Fields(fieldArtifactID).Value.Value

        If Nullable.Equals(objectToNullableOfBoolean(objPreviousValue), objectToNullableOfBoolean(objCurrentValue)) Then
          fieldChanged = False
        Else
          fieldChanged = True
        End If

      Case 5, 8 'Single choice, multiple choice
        'Compare previous choice value(s) to current value
        Dim sql As String
        Dim previousChoiceValue As New List(Of Int32)
        Dim currentChoiceValue As New List(Of Int32)

        'Get sorted previous choices in order to compare to current
        sql = String.Format("SELECT CodeArtifactID FROM [EDDSDBO].[ZCodeArtifact_{1}] CA WITH(NOLOCK) WHERE AssociatedArtifactID = {0} ORDER BY CodeArtifactID", documentArtifactID, codeTypeID)
        'sql = String.Format("SELECT CodeArtifactId FROM CodeArtifact CA WITH(NOLOCK) WHERE AssociatedArtifactID = {0} AND CodeTypeID = {1} ORDER BY CodeTypeID", documentArtifactID, codeTypeID)
        Dim ds As DataSet = dbConnection.ExecuteSqlStatementAsDataSet(sql)
        If ds IsNot Nothing AndAlso ds.Tables.Count = 1 Then
          For Each dr As DataRow In ds.Tables(0).Rows
            previousChoiceValue.Add(CType(dr("CodeArtifactID"), Int32))
          Next
        End If

        'Get sorted current choices in order to compare to previos
        Dim currentChoiceCollection As kCura.EventHandler.ChoiceCollection
        currentChoiceCollection = CType(Me.ActiveArtifact.Fields(fieldArtifactID).Value, kCura.EventHandler.ChoiceFieldValue).Choices
        For Each c As kCura.EventHandler.Choice In currentChoiceCollection
          currentChoiceValue.Add(c.ArtifactID)
        Next

        'compare previous and current choices
        If OrderedListOfInt32Equals(previousChoiceValue, currentChoiceValue) Then
          fieldChanged = False
        Else
          fieldChanged = True
        End If

      Case Else
        Throw New Exception(String.Format("{0} contains an invalid field type as a FieldToMonitor", tableName))
    End Select

    Return fieldChanged
  End Function

  Private Function OrderedListOfInt32Equals(ByVal listA As List(Of Int32), ByVal ListB As List(Of Int32)) As Boolean
    If listA.Count = ListB.Count Then
      For i As Integer = 0 To listA.Count - 1
        If listA(i) <> ListB(i) Then
          Return False
          Exit For
        End If
      Next
    Else
      Return False
    End If
    Return True
  End Function

  Private Sub UpdateModField(ByVal dbConnection As Relativity.API.DBContext, ByVal modColumnName As String, ByVal modColumnArtifactID As Int32, ByVal newValue As Object, ByVal documentArtifactID As Int32)
    Try
      If IsFieldOnActiveLayout(modColumnArtifactID) Then
        'update using Event Handler framework
        Me.ActiveArtifact.Fields(modColumnArtifactID).Value.Value = newValue
      Else
        'update with SQL
        Dim sql As String
        sql = String.Format("UPDATE Document SET [{0}] = '{1}' WHERE ArtifactID = {2}", modColumnName, newValue.ToString, documentArtifactID.ToString)
                dbConnection.ExecuteNonQuerySQLStatement(sql, 180)
      End If
    Catch ex As Exception
      Throw New Exception(String.Format("Error updating ModField:{0};{1}; with newValue:{2}", modColumnArtifactID.ToString, modColumnName, newValue), ex)
    End Try
  End Sub

  Private Sub UpdateModFieldForDate(ByVal dbConnection As Relativity.API.DBContext, ByVal modColumnName As String, ByVal modColumnArtifactID As Int32, ByVal newValue As Object, ByVal documentArtifactID As Int32)
    Try
      If IsFieldOnActiveLayout(modColumnArtifactID) Then
        'update using Event Handler framework
        Me.ActiveArtifact.Fields(modColumnArtifactID).Value.Value = New Nullable(Of Date)(CDate(newValue).ToUniversalTime)
      Else
        'update with SQL
        Dim sql As String
        sql = String.Format("UPDATE Document SET [{0}] = GetDate() WHERE ArtifactID = {1}", modColumnName, documentArtifactID.ToString)
                dbConnection.ExecuteNonQuerySQLStatement(sql, 180)
      End If
    Catch ex As Exception
      Throw New Exception(String.Format("Error updating ModField:{0};{1}; with newValue:{2}", modColumnArtifactID.ToString, modColumnName, newValue), ex)
    End Try
  End Sub

#Region " Private Classes "

  Private Class DocumentFieldEditsTracker
    Public fieldToMonitorAritfactID As Int32
    Public fieldToMonitorColumnName As String
    Public reviewedByUserFieldArtifactID As Int32
    Public reviewedOnDateFieldArtifactID As Int32
    Public trackInitialChangeOnly As Boolean
    Public reviewedByUserFieldColumnName As String
    Public reviewedOnDateFieldColumnName As String
    Public fieldTypeID As Int32
    Public codeTypeID As Int32?

    Public Sub New(ByVal dr As DataRow)
      If Int32.TryParse(CStr(dr("FieldToMonitor")), fieldToMonitorAritfactID) = False Then
        Throw New InvalidCastException("Error casting FieldToMonitor to Int32")
      End If

      fieldToMonitorColumnName = CType(dr("FieldToMonitorColumnName"), String)
      If Int32.TryParse(CStr(dr("ReviewedByUserField")), reviewedByUserFieldArtifactID) = False Then
        Throw New InvalidCastException("Error casting ReviewedByUserField to Int32")
      End If
      If Int32.TryParse(CStr(dr("ReviewedOnDateField")), reviewedOnDateFieldArtifactID) = False Then
        Throw New InvalidCastException("Error casting ReviewedOnDateField to Int32")
      End If

      If Boolean.TryParse(CStr(dr("TrackInitialChangeOnly")), trackInitialChangeOnly) = False Then
        Throw New InvalidCastException("Error casting TrackInitialChangeOnly to Boolean")
      End If

      reviewedByUserFieldColumnName = CType(dr("ReviewedByUserFieldColumnName"), String)
      reviewedOnDateFieldColumnName = CType(dr("ReviewedOnDateFieldColumnName"), String)

      If Int32.TryParse(CStr(dr("FieldTypeID")), fieldTypeID) = False Then
        Throw New InvalidCastException("Error casting FieldTypeID to Int32")
      End If

      Try
        If IsDBNull(dr("CodeTypeID")) Then
          codeTypeID = New Nullable(Of Int32)
        Else
          codeTypeID = New Nullable(Of Int32)(CType(dr("CodeTypeID"), Int32))
        End If

      Catch ex As Exception
        Throw New InvalidCastException("Error casting CodeTypeID to Int32?")
      End Try
    End Sub
  End Class

  Private Class DocumentFieldEditsTrackerHolder
    Private triggerData As New DataTable
    Private databaseConnection As Relativity.API.DBContext
    Public hasData As Boolean
    Public htTriggerInfo As Hashtable

    Public Sub New(ByVal dbConn As Relativity.API.DBContext)
      databaseConnection = dbConn
      RetrieveTriggerInformation()
      If triggerData.Rows.Count > 0 Then
        hasData = True
        PopulateTriggerField()
      Else
        hasData = False
        htTriggerInfo = New Hashtable
      End If
    End Sub

    Private Sub RetrieveTriggerInformation()
      Dim sql As String
      Dim ds As DataSet
      sql = String.Format(<sql><![CDATA[
SELECT FieldToMonitor, ReviewedOnDateField, ReviewedByUserField, TrackInitialChangeOnly , AVF.ColumnName as FieldToMonitorColumnName, AVF2.ColumnName as ReviewedOnDateFieldColumnName, AVF3.ColumnName as ReviewedByUserFieldColumnName, F.FieldTypeID as FieldTypeID, F.CodeTypeID as CodeTypeID
FROM {0} DFET WITH(NOLOCK) 
	INNER JOIN Field F WITH(NOLOCK) ON DFET.FieldToMonitor = F.ArtifactID
	INNER JOIN ArtifactViewField AVF WITH(NOLOCK) ON AVF.ArtifactViewFieldID = F.ArtifactViewFieldID 
	INNER JOIN Field F2 WITH(NOLOCK) ON DFET.ReviewedOnDateField = F2.ArtifactID
	INNER JOIN ArtifactViewField AVF2 WITH(NOLOCK) ON AVF2.ArtifactViewFieldID = F2.ArtifactViewFieldID 
	INNER JOIN Field F3 WITH(NOLOCK) ON DFET.ReviewedByUserField = F3.ArtifactID
	INNER JOIN ArtifactViewField AVF3 WITH(NOLOCK) ON AVF3.ArtifactViewFieldID = F3.ArtifactViewFieldID 
]]></sql>.Value, tableName)
      ds = databaseConnection.ExecuteSqlStatementAsDataSet(sql)
      If ds IsNot Nothing AndAlso ds.Tables.Count = 1 Then
        triggerData = ds.Tables(0)
      End If
    End Sub

    Private Sub PopulateTriggerField()
      htTriggerInfo = New Hashtable
      For Each dr As DataRow In triggerData.Rows
        Try
          htTriggerInfo.Add(CType(dr("FieldToMonitor"), Int32), New DocumentFieldEditsTracker(dr))
        Catch ex As System.ArgumentException
          Throw New Exception(String.Format("Document Field Edits Tracker contains multiple records for the field with ArtifactID {0}. Please contact your system administrator.", dr("FieldToMonitor").ToString), ex)
        Catch ex As InvalidCastException
          Throw New Exception(String.Format("{0} contains invalid data in the one of its columns", tableName), ex)
        End Try
      Next
    End Sub
  End Class
#End Region

End Class

