import React from 'react';
import ComponentCreator from '@docusaurus/ComponentCreator';

export default [
  {
    path: '/schemax-vscode/',
    component: ComponentCreator('/schemax-vscode/', '3df'),
    routes: [
      {
        path: '/schemax-vscode/',
        component: ComponentCreator('/schemax-vscode/', '971'),
        routes: [
          {
            path: '/schemax-vscode/',
            component: ComponentCreator('/schemax-vscode/', '107'),
            routes: [
              {
                path: '/schemax-vscode/guide/architecture/',
                component: ComponentCreator('/schemax-vscode/guide/architecture/', '739'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schemax-vscode/guide/development/',
                component: ComponentCreator('/schemax-vscode/guide/development/', '678'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schemax-vscode/guide/quickstart/',
                component: ComponentCreator('/schemax-vscode/guide/quickstart/', 'f2b'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schemax-vscode/reference/cli/',
                component: ComponentCreator('/schemax-vscode/reference/cli/', 'e89'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schemax-vscode/reference/provider-contract/',
                component: ComponentCreator('/schemax-vscode/reference/provider-contract/', 'e7b'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schemax-vscode/reference/test-coverage-checklist/',
                component: ComponentCreator('/schemax-vscode/reference/test-coverage-checklist/', '482'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schemax-vscode/reference/testing/',
                component: ComponentCreator('/schemax-vscode/reference/testing/', 'c0b'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schemax-vscode/reference/workflows/',
                component: ComponentCreator('/schemax-vscode/reference/workflows/', 'f47'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schemax-vscode/',
                component: ComponentCreator('/schemax-vscode/', 'b7d'),
                exact: true,
                sidebar: "tutorialSidebar"
              }
            ]
          }
        ]
      }
    ]
  },
  {
    path: '*',
    component: ComponentCreator('*'),
  },
];
