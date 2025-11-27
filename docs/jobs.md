Esse projeto utiliza um Job no Databricks para orquestrar a execução automática dos notebooks responsáveis pelas camadas do pipeline de dados (Landing, Bronze, Silver e Gold).
A seguir está a explicação detalhada de cada etapa, baseada no JSON de configuração do job:

```
 {

  "name": "Pipeline_Vinhos",
  "max_concurrent_runs": 1,
  "timeout_seconds": 0,
  "performance_target": "PERFORMANCE_OPTIMIZED"
}
```
O job se chama Pipeline_Vinhos e garante que somente uma execução pode ocorrer por vez (max_concurrent_runs: 1).
A execução é otimizada para performance pelo Databricks.

Fluxo Geral dos Jobs

O pipeline executa uma sequência de 5 tarefas, onde cada uma depende do sucesso da anterior.
A ordem é:
Criação das Tabelas
Conexão: Banco ➝ Landing
Landing ➝ Bronze
Bronze ➝ Silver
Silver ➝ Gold

Abaixo cada etapa em detalhes.

1.Criacao_das_tabelas

```
{
  "task_key": "Criacao_das_tabelas",
  "notebook_task": {
    "notebook_path": "/Repos/.../Vinhos 001"
  }
}
```

2.Conexao_Banco-para-Landing
```
{
  "task_key": "Conexao_Banco-para-Landing",
  "depends_on": [{"task_key": "Criacao_das_tabelas"}],
  "notebook_task": {
    "notebook_path": "/Repos/.../Conexao"
  }
}
```

3.Landing_para_Bronze

```
{
  "task_key": "Landing_para_Bronze",
  "depends_on": [{"task_key": "Conexao_Banco-para-Landing"}],
  "notebook_task": {
    "notebook_path": "/Repos/.../Vinhos 002"
  }
}
```

4.Bronze_para_Silver

```
{
  "task_key": "Bronze_para_Silver",
  "depends_on": [{"task_key": "Landing_para_Bronze"}],
  "notebook_task": {
    "notebook_path": "/Repos/.../Vinhos 003 - Silver"
  }
}
```

5.Silver_para_Gold

```
{
  "task_key": "Silver_para_Gold",
  "depends_on": [{"task_key": "Bronze_para_Silver"}],
  "notebook_task": {
    "notebook_path": "/Repos/.../Vinhos 004 - Gold"
  }
}
```

- Notificações, Webhooks e Ambiente

O job possui:
Notificações de e-mail configuráveis
Execução otimizada
Ambiente padrão "Default"

```
{
  "environment_key": "Default",
  "spec": {
    "environment_version": "4"
  }
}
```



